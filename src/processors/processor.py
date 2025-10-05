import asyncio
import json
import re
import csv
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class FileConverter:
    def __init__(
        self,
        raw_dir: Path | str = None,
        processed_dir: Path | str = None,
        outputs_dir: Path | str = None,
        concurrency: int = 4,
    ) -> None:
        base = Path(__file__).resolve().parents[2]
        self.raw_dir = Path(raw_dir) if raw_dir else base / "data" / "raw"
        self.processed_dir = Path(processed_dir) if processed_dir else base / "data" / "processed"
        self.outputs_dir = Path(outputs_dir) if outputs_dir else base / "data" / "outputs"
        self.concurrency = max(1, concurrency)

        self.processed_dir.mkdir(parents=True, exist_ok=True)
        (self.outputs_dir / "csv").mkdir(parents=True, exist_ok=True)
        (self.outputs_dir / "parquet").mkdir(parents=True, exist_ok=True)

    async def run(self) -> Dict[str, Dict[str, str]]:
        json_files = list(self.raw_dir.glob("*.json"))
        if not json_files:
            logger.info("No JSON files found in %s", self.raw_dir)
            return {}

        sem = asyncio.Semaphore(self.concurrency)

        async def _sem_task(p: Path):
            async with sem:
                return await self._process_file_async(p)

        tasks = [asyncio.create_task(_sem_task(p)) for p in json_files]
        results = await asyncio.gather(*tasks)

        # map filename -> outputs
        outputs = {k: v for k, v in results if k}
        logger.info("Finished converting %d files", len(outputs))
        return outputs

    async def _process_file_async(self, path: Path) -> Tuple[str, Dict[str, str]]:
        # Delegate blocking pandas work to a thread
        try:
            out = await asyncio.to_thread(self._process_file_sync, path)
            return path.name, out
        except Exception as exc:
            logger.exception("Failed to process %s: %s", path, exc)
            return path.name, {}

    def _process_file_sync(self, path: Path) -> Dict[str, str]:
        df = None
        records = None
        try:
            # try newline-delimited JSON first
            df = pd.read_json(path, lines=True)
        except Exception:
            try:
                df = pd.read_json(path)
            except Exception:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                    records = data["data"]
                elif isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    # try to find a list inside the dict
                    for v in data.values():
                        if isinstance(v, list):
                            records = v
                            break

                if records is not None:
                    df = pd.json_normalize(records)
                else:
                    # fallback to normalizing the whole dict
                    df = pd.json_normalize(data)

        if df is None:
            df = pd.DataFrame()


        try:
            if "data" in df.columns:
                # collect all list-like entries from the 'data' column
                lists = df["data"].apply(lambda x: x if isinstance(x, list) else [])
                all_records = []
                for lst in lists:
                    all_records.extend(lst)
                if all_records:
                    df = pd.json_normalize(all_records)
                else:
                    # fallback to expanding any nested dicts inside 'data'
                    try:
                        df = pd.json_normalize(df.to_dict(orient="records"))
                    except Exception:
                        pass
            else:
                df = pd.json_normalize(df.to_dict(orient="records"))
        except Exception: pass

        name = path.stem
        data_prefixed = [c for c in df.columns if c.startswith("data.")]
        if data_prefixed:
            df = df[data_prefixed].rename(columns={c: c.split("data.", 1)[1] for c in data_prefixed})

        # Remove common outer metadata columns if present
        for meta_col in ("url", "status_code", "timestamp", "size", "scraped_at"):
            if meta_col in df.columns:
                try:
                    df = df.drop(columns=[meta_col])
                except Exception: pass

        # json_placeholder users: flatten address into one column (remove geo) and flatten company
        if "json_placeholder" in name or name.startswith("json_placeholder"):
            try:
                # if address parts exist as nested columns like 'address.street'
                addr_cols = [c for c in df.columns if c.startswith("address.")]
                street = df.get("address.street")
                suite = df.get("address.suite")
                city = df.get("address.city")
                zipcode = df.get("address.zipcode")

                if any(col is not None for col in [street, suite, city, zipcode]):
                    def _make_address(row):
                        parts = []
                        for col in ("address.street", "address.suite", "address.city", "address.zipcode"):
                            v = row.get(col)
                            if pd.notna(v) and v not in (None, ""):
                                parts.append(str(v))
                        return ", ".join(parts)

                    df["address"] = df.to_dict(orient="records")
                    df["address"] = df["address"].apply(_make_address)

                    # drop all address.* columns
                    df = df[[c for c in df.columns if not c.startswith("address.")]]

                # company flattening
                if any(c.startswith("company.") for c in df.columns):
                    # create explicit company fields
                    df["company_name"] = df.get("company.name")
                    df["company_catchPhrase"] = df.get("company.catchPhrase")
                    df["company_bs"] = df.get("company.bs")
                    # drop nested company.* columns
                    df = df[[c for c in df.columns if not c.startswith("company.")]]
            except Exception:
                logger.exception("Failed to flatten json_placeholder users for %s", path.name)

        # reddit posts: convert created_utc to human readable time
        if "reddit" in name or "created_utc" in df.columns:
            try:
                if "created_utc" in df.columns:
                    # created_utc may be float seconds since epoch
                    df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                logger.exception("Failed to convert created_utc for %s", path.name)

        # Sanitize string columns to avoid embedded newlines harming CSV format
        def _sanitize_text(val):
            if not isinstance(val, str):
                return val
            # replace CRLF / LF with a single space, collapse whitespace
            v = val.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
            v = re.sub(r"\s+", " ", v).strip()
            return v

        try:
            obj_cols = df.select_dtypes(include=["object"]).columns
            for c in obj_cols:
                df[c] = df[c].apply(_sanitize_text)
        except Exception:
            # if sanitization fails, continue without failing the whole file
            logger.exception("Failed to sanitize text columns for %s", path.name)

        name = path.stem
        csv_path = self.processed_dir / f"{name}.csv"
        parquet_path = self.processed_dir / f"{name}.parquet"

        # Also place formatted outputs in outputs/<format>/
        out_csv = self.outputs_dir / "csv" / f"{name}.csv"
        out_parquet = self.outputs_dir / "parquet" / f"{name}.parquet"

        # Write CSV
        try:
            df.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
            df.to_csv(out_csv, index=False, quoting=csv.QUOTE_MINIMAL)
        except Exception:
            # last-resort: write raw json lines
            logger.exception("Failed to write CSV for %s", name)

        # Write Parquet
        try:
            df.to_parquet(parquet_path, index=False)
            df.to_parquet(out_parquet, index=False)
        except Exception:
            logger.exception("Failed to write Parquet for %s", name)

        logger.info("Wrote %s -> %s (csv, parquet)", path.name, self.processed_dir)
        return {
            "csv": str(csv_path),
            "parquet": str(parquet_path),
            "outputs_csv": str(out_csv),
            "outputs_parquet": str(out_parquet),
        }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import asyncio

    fc = FileConverter()
    asyncio.run(fc.run())
