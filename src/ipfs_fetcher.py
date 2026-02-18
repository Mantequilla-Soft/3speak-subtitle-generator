"""
IPFS Video Fetcher
Downloads videos from IPFS gateways with fallback support
"""

import os
import logging
import subprocess
import requests
from typing import Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class IPFSFetcher:
    """Handles downloading videos from IPFS"""

    def __init__(self, config: dict):
        """Initialize IPFS fetcher with configuration"""
        self.gateways = config['ipfs']['gateways']
        self.timeout = config['ipfs']['timeout']
        self.ffmpeg_timeout = config['ipfs'].get('ffmpeg_timeout', 3600)
        self.ipfs_api_url = config['ipfs'].get('api_url', 'http://localhost:5001')
        self.temp_dir = Path('/app/temp')
        self.temp_dir.mkdir(exist_ok=True)

    def download_video(self, cid: str, author: str, permlink: str) -> Optional[str]:
        """
        Download video from IPFS using available gateways

        Args:
            cid: IPFS content identifier
            author: Video author
            permlink: Video permlink

        Returns:
            Path to downloaded video file, or None if failed
        """
        # Create safe filename
        filename = f"{author}_{permlink}_{cid[:8]}.mp4"
        output_path = self.temp_dir / filename

        # Skip if already exists
        if output_path.exists():
            logger.info(f"Video already exists: {filename}")
            return str(output_path)

        # Try each gateway
        for gateway in self.gateways:
            url = f"{gateway}{cid}"
            logger.info(f"Attempting to download from: {url}")

            try:
                response = requests.get(
                    url,
                    stream=True,
                    timeout=self.timeout
                )
                response.raise_for_status()

                # Download with progress
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0

                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)

                            # Log progress every 10MB
                            if downloaded % (10 * 1024 * 1024) == 0:
                                mb_downloaded = downloaded / (1024 * 1024)
                                logger.info(f"Downloaded {mb_downloaded:.1f} MB...")

                file_size = output_path.stat().st_size / (1024 * 1024)
                logger.info(f"Successfully downloaded: {filename} ({file_size:.1f} MB)")
                return str(output_path)

            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to download from {gateway}: {e}")
                continue

            except Exception as e:
                logger.error(f"Unexpected error downloading from {gateway}: {e}")
                continue

        logger.error(f"Failed to download video {cid} from all gateways")
        return None

    def download_hls_video(self, manifest_cid: str, author: str, permlink: str) -> Optional[str]:
        """
        Download an HLS video (embed-video collection) by fetching the m3u8 manifest
        via ffmpeg with gateway fallback.

        Args:
            manifest_cid: The manifest CID (without ipfs:// prefix)
            author: Video author
            permlink: Video permlink

        Returns:
            Path to downloaded MP4 file, or None if all gateways failed
        """
        filename = f"{author}_{permlink}_{manifest_cid[:8]}.mp4"
        output_path = self.temp_dir / filename

        if output_path.exists():
            logger.info(f"HLS video already exists: {filename}")
            return str(output_path)

        for gateway in self.gateways:
            manifest_url = f"{gateway}{manifest_cid}/manifest.m3u8"
            logger.info(f"Attempting HLS download from: {manifest_url}")

            try:
                cmd = [
                    'ffmpeg', '-y',
                    '-i', manifest_url,
                    '-c', 'copy',
                    str(output_path)
                ]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    timeout=self.ffmpeg_timeout
                )

                if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
                    file_size = output_path.stat().st_size / (1024 * 1024)
                    logger.info(f"Successfully downloaded HLS: {filename} ({file_size:.1f} MB)")
                    return str(output_path)
                else:
                    logger.warning(f"ffmpeg failed for {manifest_url}: "
                                   f"{result.stderr.decode(errors='replace')[:300]}")
                    if output_path.exists():
                        output_path.unlink()

            except subprocess.TimeoutExpired:
                logger.warning(f"HLS download timed out ({self.ffmpeg_timeout}s) from {gateway}")
                if output_path.exists():
                    output_path.unlink()
            except Exception as e:
                logger.warning(f"Error downloading HLS from {gateway}: {e}")
                if output_path.exists():
                    output_path.unlink()

        logger.error(f"Failed to download HLS for manifest_cid={manifest_cid}")
        return None

    def cleanup_video(self, video_path: str) -> bool:
        """
        Delete temporary video file

        Args:
            video_path: Path to video file

        Returns:
            True if successful
        """
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
                logger.info(f"Cleaned up: {os.path.basename(video_path)}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error cleaning up {video_path}: {e}")
            return False

    def add_to_ipfs(self, file_path: str) -> Optional[str]:
        """
        Add a file to the local IPFS node.
        The node pins it automatically on add.

        Returns:
            IPFS CID string, or None on failure.
        """
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(
                    f"{self.ipfs_api_url}/api/v0/add",
                    files={'file': f},
                    timeout=30
                )
            response.raise_for_status()
            cid = response.json()['Hash']
            logger.info(f"Added to IPFS: {cid} ({os.path.basename(file_path)})")
            return cid
        except Exception as e:
            logger.error(f"Failed to add {file_path} to IPFS: {e}")
            return None

    def pin_remote(self, cid: str, remote_url: str) -> bool:
        """
        Pin a CID on a remote IPFS node.

        Args:
            cid: The IPFS CID to pin.
            remote_url: Full base URL of the remote pin endpoint
                        (e.g. https://ipfs.3speak.tv/api/v0/pin/add)

        Returns:
            True if the remote accepted the pin.
        """
        try:
            response = requests.post(
                f"{remote_url}?arg={cid}",
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"Pinned remotely: {cid}")
            return True
        except Exception as e:
            logger.warning(f"Remote pin failed for {cid}: {e}")
            return False

    def get_video_info(self, video_path: str) -> dict:
        """
        Get video information using ffprobe

        Args:
            video_path: Path to video file

        Returns:
            Dictionary with video info
        """
        try:
            import ffmpeg

            probe = ffmpeg.probe(video_path)
            video_info = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'video'),
                None
            )
            audio_info = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'audio'),
                None
            )

            return {
                'duration': float(probe['format'].get('duration', 0)),
                'size': int(probe['format'].get('size', 0)),
                'video_codec': video_info.get('codec_name') if video_info else None,
                'audio_codec': audio_info.get('codec_name') if audio_info else None,
                'has_audio': audio_info is not None
            }

        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return {}
