# Simple File Downloader (v8.7)
#
# - Added graceful shutdown on Ctrl+C (KeyboardInterrupt).
# - When interrupted, the script will stop finding new files and finish any in-progress downloads before exiting.
#
# To install dependencies:
# pip install aiohttp asyncio aiofiles beautifulsoup4 tqdm colorama lxml

import asyncio
import aiofiles
import os
import time
import logging
import re
from pathlib import Path
from typing import List, Set, Optional
from urllib.parse import urljoin, unquote, urlparse
from dataclasses import dataclass, field

import aiohttp
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from colorama import Fore, Style, init
from tqdm.contrib.logging import _TqdmLoggingHandler as TqdmLoggingHandler

# --- CONFIGURATION ---
@dataclass
class DownloaderConfig:
    """Configuration settings for the downloader."""
    url: str = "https://elhacker.info/Cursos/Cybrary%20-%20Become%20a%20SOC%20Analyst%20-%20Level%203%20Path/"
    file_extensions: List[str] = field(default_factory=lambda: [".txt", ".md", ".mp4", ".srt", ".rar", ".zip", ".pdf", ".webm"])
    base_download_folder: str = "downloads"
    
    max_concurrent_downloads: int = 4
    request_timeout: int = 180
    
    chunk_size: int = 8192
    max_retries: int = 3
    retry_delay: float = 1.0
    max_depth: int = 10
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# --- LOGGING SETUP ---
init(autoreset=True)
class ColoredFormatter(logging.Formatter):
    COLORS = {'WARNING': Fore.YELLOW, 'INFO': Fore.CYAN, 'DEBUG': Fore.BLUE, 'CRITICAL': Fore.RED, 'ERROR': Fore.RED}
    def format(self, record):
        log_message = super().format(record)
        color = self.COLORS.get(record.levelname, '')
        return f"{color}{log_message}{Style.RESET_ALL}"

def setup_logging(log_folder: str = "logs") -> logging.Logger:
    Path(log_folder).mkdir(exist_ok=True)
    logger = logging.getLogger("downloader")
    if logger.hasHandlers(): logger.handlers.clear()
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(Path(log_folder) / f"downloader_{int(time.time())}.log", mode='w', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'))
    logger.addHandler(file_handler)
    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(ColoredFormatter('%(levelname)s: %(message)s'))
    logger.addHandler(console_handler)
    return logger

# --- UTILITY FUNCTIONS ---
def sanitize_filename(name: str) -> str:
    name = unquote(name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip(' .')
    return name[:255]

def is_file_url(url: str, extensions: List[str]) -> bool:
    return any(url.lower().endswith(ext.lower()) for ext in extensions)

def is_directory_url(url: str, extensions: List[str]) -> bool:
    if is_file_url(url, extensions): return False
    if ('?' in url and not url.endswith('?')) or ('#' in url and not url.endswith('#')): return False
    if url.endswith('/'): return True
    path_part = url.split('/')[-1]
    if '.' not in path_part or path_part.endswith('.'): return True
    return False

# --- MAIN DOWNLOADER CLASS ---
@dataclass
class FileInfo:
    url: str
    local_path: Path
    size: Optional[int] = None

class SimpleFileDownloader:
    def __init__(self, config: DownloaderConfig):
        self.config = config
        self.logger = setup_logging()
        self.start_url = config.url if config.url.endswith('/') else f"{config.url}/"
        self.project_folder = self._create_project_folder()
        self.session: Optional[aiohttp.ClientSession] = None
        # --- NEW: Event to signal graceful shutdown ---
        self.shutdown_event = asyncio.Event()
        self._worker_tasks: List[asyncio.Task] = []

    def _create_project_folder(self) -> Path:
        try:
            parsed_url = urlparse(self.start_url)
            project_name = sanitize_filename(parsed_url.path.strip('/').split('/')[-1] or "downloaded_files")
            project_path = Path(self.config.base_download_folder) / project_name
            project_path.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Project folder: {project_path.absolute()}")
            return project_path
        except Exception as e:
            self.logger.error(f"Failed to create project folder: {e}")
            fallback_path = Path(self.config.base_download_folder) / "downloaded_files"
            fallback_path.mkdir(parents=True, exist_ok=True)
            return fallback_path

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10, ttl_dns_cache=300, use_dns_cache=True)
        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers={"User-Agent": self.config.user_agent}, trust_env=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session: await self.session.close()

    async def _fetch_html(self, url: str) -> Optional[str]:
        if is_file_url(url, self.config.file_extensions): return None
        for attempt in range(self.config.max_retries + 1):
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    content_type = response.headers.get('content-type', '').lower()
                    if 'text/html' not in content_type and 'text/plain' not in content_type: return None
                    try: return await response.text(encoding='utf-8')
                    except UnicodeDecodeError: return (await response.read()).decode('latin1', errors='replace')
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                self.logger.warning(f"Error fetching {url}: {e} (attempt {attempt + 1})")
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url}: {e}"); break
            if attempt < self.config.max_retries: await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
        return None

    async def _discover_recursively(self, directory_url: str, current_depth: int, found_files: List[FileInfo], visited_urls: Set[str]):
        # --- NEW: Stop discovery if shutdown is requested ---
        if self.shutdown_event.is_set():
            return
            
        if current_depth > self.config.max_depth or directory_url in visited_urls: return
        visited_urls.add(directory_url)
        self.logger.info(f"Scanning [Depth {current_depth}]: {unquote(directory_url)}")
        html = await self._fetch_html(directory_url)
        if not html: return
        try:
            soup = BeautifulSoup(html, 'lxml')
            sub_tasks = []
            for link in soup.find_all('a', href=True):
                # --- NEW: Check event in loop to exit faster ---
                if self.shutdown_event.is_set():
                    break
                
                href, link_text = link['href'], link.get_text(strip=True).lower()
                if any(p in link_text for p in ['parent directory', '..']) or href.startswith('?') or href.strip() in ('../', '..', './'): continue
                absolute_url = urljoin(directory_url, href)
                if not absolute_url.startswith(self.start_url): continue
                if is_file_url(absolute_url, self.config.file_extensions):
                    relative_path_str = unquote(absolute_url.replace(self.start_url, "")).strip('/')
                    sanitized_parts = [sanitize_filename(part) for part in relative_path_str.split('/')]
                    local_path = self.project_folder.joinpath(*sanitized_parts)
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    found_files.append(FileInfo(url=absolute_url, local_path=local_path))
                elif is_directory_url(absolute_url, self.config.file_extensions) and absolute_url not in visited_urls:
                    task = asyncio.create_task(self._discover_recursively(absolute_url, current_depth + 1, found_files, visited_urls))
                    sub_tasks.append(task)
            
            if sub_tasks and not self.shutdown_event.is_set():
                await asyncio.gather(*sub_tasks)
        except Exception as e: self.logger.error(f"Failed to parse or process {directory_url}: {e}")

    async def get_file_size(self, url: str) -> Optional[int]:
        try:
            async with self.session.head(url) as response:
                if response.status == 200: return int(response.headers.get('content-length', 0))
        except Exception: pass
        return None

    async def should_skip_download(self, file_info: FileInfo) -> bool:
        if not file_info.local_path.exists(): return False
        remote_size = await self.get_file_size(file_info.url)
        if remote_size and file_info.local_path.stat().st_size == remote_size:
            self.logger.debug(f"Skipping {file_info.local_path.name} (already downloaded)"); return True
        return False
        
    async def download_file(self, file_info: FileInfo, pbar: tqdm):
        try:
            async with self.session.get(file_info.url) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                pbar.total = total_size
                pbar.refresh()
                async with aiofiles.open(file_info.local_path, 'wb') as file:
                    async for chunk in response.content.iter_chunked(self.config.chunk_size):
                        await file.write(chunk)
                        pbar.update(len(chunk))
                return True
        except Exception as e:
            self.logger.error(f"Failed to save to {file_info.local_path}. Reason: {repr(e)}")
            pbar.set_description(f"Failed: {file_info.local_path.name[:35]:<35}")
            if file_info.local_path.exists():
                try: file_info.local_path.unlink()
                except OSError: pass
            return False

    async def download_all_files(self, all_files: List[FileInfo]):
        if self.shutdown_event.is_set(): return
        if not all_files: self.logger.warning("No files to download"); return
        
        self.logger.info(f"Checking {len(all_files)} files against local copies...")
        files_to_download = [f for f in all_files if not await self.should_skip_download(f)]
        if not files_to_download: self.logger.info("All files are already up to date."); return
        
        self.logger.info(f"Downloading {len(files_to_download)} new/updated files")
        successful_downloads = 0
        with tqdm(total=len(files_to_download), unit="file", desc="Overall Progress", position=0) as overall_pbar:
            async def worker(queue: asyncio.Queue, worker_id: int):
                nonlocal successful_downloads
                # --- NEW: Loop condition checks for shutdown signal ---
                while not self.shutdown_event.is_set():
                    try:
                        # --- NEW: Use timeout to unblock periodically and re-check shutdown event ---
                        file_info = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # If queue is empty, other workers might still be processing.
                        # We rely on queue.join() to signal when all work is truly done.
                        continue
                    except asyncio.CancelledError:
                        break # Exit if the worker task itself is cancelled.

                    try: # Wrap download to ensure task_done is always called
                        file_name = file_info.local_path.name
                        with tqdm(total=0, unit='B', unit_scale=True, unit_divisor=1024,
                                  desc=f"{file_name[:35]:<35}", position=worker_id + 1, leave=False) as pbar:
                            success = await self.download_file(file_info, pbar)
                            if success:
                                successful_downloads += 1
                    finally:
                        overall_pbar.update(1)
                        queue.task_done()

            download_queue = asyncio.Queue()
            for f in files_to_download:
                await download_queue.put(f)

            # --- NEW: Store worker tasks to manage them during shutdown ---
            self._worker_tasks = [
                asyncio.create_task(worker(download_queue, i))
                for i in range(self.config.max_concurrent_downloads)
            ]

            # Wait for the queue to be fully processed. This can be interrupted.
            await download_queue.join()

        self.logger.info(f"Download complete: {successful_downloads}/{len(files_to_download)} files successful")
    
    async def run(self):
        start_time = time.time()
        try:
            self.logger.info(f"Starting recursive discovery (max depth: {self.config.max_depth})...")
            all_files, visited_urls = [], set()
            await self._discover_recursively(self.start_url, 0, all_files, visited_urls)
            
            if self.shutdown_event.is_set():
                self.logger.warning("Discovery was interrupted by user.")
                return

            unique_files = {str(f.local_path): f for f in all_files}
            unique_files_list = list(unique_files.values())
            self.logger.info(f"Total unique files found: {len(unique_files_list)}")
            await self.download_all_files(unique_files_list)
        
        # --- NEW: Centralized exception handling for graceful shutdown ---
        except (KeyboardInterrupt, asyncio.CancelledError):
            self.logger.warning("\nProcess interrupted by user. Finishing current downloads...")
            self.shutdown_event.set() # Signal all coroutines to stop gracefully
        
        except Exception as e:
            self.logger.critical(f"A critical error occurred: {e}", exc_info=True)
            self.shutdown_event.set() # Also trigger shutdown on other critical errors

        finally:
            # --- NEW: Wait for any running downloads to complete before exiting ---
            if self._worker_tasks:
                self.logger.info("Waiting for active downloads to complete...")
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)
                self.logger.info("All running downloads have been closed.")
            
            execution_time = time.time() - start_time
            self.logger.info(f"\nTotal execution time: {execution_time:.2f} seconds")
            self.logger.info(f"Downloads saved to: {self.project_folder.absolute()}")

# --- MAIN EXECUTION ---
async def main():
    config = DownloaderConfig()
    async with SimpleFileDownloader(config) as downloader:
        await downloader.run()

if __name__ == "__main__":
    if os.name == 'nt': asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        # asyncio.run handles KeyboardInterrupt by raising CancelledError in the task
        asyncio.run(main())
    except KeyboardInterrupt:
        # This message will appear if Ctrl+C is pressed before the event loop starts
        print("\nDownloader stopped by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred in the main execution block: {e}")
        import traceback; traceback.print_exc()
