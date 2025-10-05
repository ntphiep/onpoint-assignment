import json
import logging
from pathlib import Path
import shutil
from datetime import datetime, timezone
from typing import Dict

try:
    from ..utils.config import config
except Exception:
    # fallback when running as a script
    import sys
    from pathlib import Path as _P

    sys.path.append(str(_P(__file__).parent.parent.parent))
    from src.utils.config import config

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class S3Uploader:
    def __init__(
        self,
        processed_dir: str,
        bucket: str,
        region: str,
        access_key: str,
        secret_key: str,
        simulate: bool = False,
    ) -> None:
        base = Path(__file__).resolve().parents[2]
        self.processed_dir = Path(processed_dir) if processed_dir else base / "data" / "processed"
        self.simulate = bool(simulate)
        # load from config if values not provided
        aws_cfg = config.get("aws", {}) if config is not None else {}
        self.bucket = bucket or aws_cfg.get("s3_bucket") or aws_cfg.get("bucket")
        self.region = region or aws_cfg.get("region")
        access_key = access_key or aws_cfg.get("access_key") or aws_cfg.get("aws_access_key_id")
        secret_key = secret_key or aws_cfg.get("secret_key") or aws_cfg.get("aws_secret_access_key")

        # boto3 client only if not simulating
        self.s3 = None
        if not self.simulate:
            try:
                if access_key and secret_key:
                    session = boto3.session.Session(
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key,
                        region_name=self.region,
                    )
                    self.s3 = session.client("s3")
                else:
                    self.s3 = boto3.client("s3", region_name=self.region)

                if not self.bucket:
                    logger.warning("No S3 bucket configured; switching to simulate mode")
                    self.s3 = None
                    self.simulate = True
            except Exception:
                logger.exception("Failed to initialize boto3 S3 client; falling back to simulate mode")
                self.s3 = None
                self.simulate = True

        # for local fallback target
        self.target_dir = base / "data" / "s3_simulation"
        self.target_dir.mkdir(parents=True, exist_ok=True)

    def upload_processed_data(self) -> Dict[str, bool]:
        results = {}
        files = list(self.processed_dir.glob("**/*"))
        files = [f for f in files if f.is_file()]
        if not files:
            logger.info("No processed files found in %s", self.processed_dir)
            return results

        for f in files:
            rel = f.relative_to(self.processed_dir)
            key = str(rel).replace("\\", "/")
            try:
                if self.simulate or not self.s3:
                    dest = self.target_dir / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(f, dest)
                    # write metadata
                    meta = {"original_path": str(f), "uploaded_at": datetime.now(timezone.utc).isoformat(), "size": f.stat().st_size}
                    with open(dest.with_suffix(dest.suffix + ".metadata"), "w", encoding="utf-8") as mf:
                        json.dump(meta, mf, indent=2)
                    results[key] = True
                else:
                    self.s3.upload_file(str(f), self.bucket, key)
                    results[key] = True
            except (BotoCoreError, ClientError) as be:
                logger.exception("Boto3 upload failed for %s", f)
                results[key] = False
            except Exception:
                logger.exception("Failed to upload %s", f)
                results[key] = False

        # write a report locally
        report = {"generated_at": datetime.now(timezone.utc).isoformat(), "uploaded_files": [{"path": k, "success": v} for k, v in results.items()]}
        report_path = self.target_dir / "upload_report.json"
        with open(report_path, "w", encoding="utf-8") as rf:
            json.dump(report, rf, indent=2)

        logger.info("Upload complete: %d files", len(results))
        return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    uploader = S3Uploader(simulate=False)
    res = uploader.upload_processed_data()
    print(f"Upload results: {len(res)} files")
