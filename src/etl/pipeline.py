import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


class ETLPipeline:
	def __init__(self, raw_dir: str | Path = None, sql_out: str | Path = None):
		base = Path(__file__).resolve().parents[2]
		self.raw_dir = Path(raw_dir) if raw_dir else base / "data" / "raw"
		self.sql_out = Path(sql_out) if sql_out else base / "src" / "etl" / "generated_upserts.sql"

	def _load_json_records(self, path: Path):
		with open(path, "r", encoding="utf-8") as f:
			data = json.load(f)

		# prefer top-level 'data' list
		if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
			records = data["data"]
		elif isinstance(data, list):
			records = data
		else:
			# attempt to find first list value
			records = None
			if isinstance(data, dict):
				for v in data.values():
					if isinstance(v, list):
						records = v
						break
			if records is None:
				# fallback: wrap the dict
				records = [data]

		df = pd.json_normalize(records)
		return df

	def _sanitize_sql_literal(self, val):
		if pd.isna(val):
			return "NULL"
		if isinstance(val, (int, float)) and not isinstance(val, bool):
			return str(val)
		if isinstance(val, bool):
			return 'TRUE' if val else 'FALSE'
		s = str(val)
		# collapse newlines and escape single quotes
		s = s.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
		s = s.replace("'", "''")
		return f"'{s}'"

	def _process_json_placeholder(self, df: pd.DataFrame) -> pd.DataFrame:
		# Flatten address into single column
		try:
			def make_address(row):
				parts = []
				for col in ("address.street", "address.suite", "address.city", "address.zipcode"):
					v = row.get(col)
					if pd.notna(v) and v not in (None, ""):
						parts.append(str(v))
				return ", ".join(parts)

			df["address"] = df.apply(lambda r: make_address(r.to_dict()), axis=1)
		except Exception:
			logger.exception("address flatten failed")

		# Flatten company
		df["company_name"] = df.get("company.name")
		df["company_catchPhrase"] = df.get("company.catchPhrase")
		df["company_bs"] = df.get("company.bs")

		# Select desired columns and rename if needed
		cols = [c for c in ("id", "name", "username", "email", "phone", "website", "address", "company_name", "company_catchPhrase", "company_bs") if c in df.columns]
		return df[cols]

	def _process_reddit(self, df: pd.DataFrame) -> pd.DataFrame:
		# convert created_utc seconds -> UTC timestamp string
		if "created_utc" in df.columns:
			try:
				df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")
			except Exception:
				logger.exception("failed to convert created_utc")

		cols = [c for c in ("id", "title", "author", "score", "num_comments", "created_utc", "selftext", "subreddit", "upvote_ratio", "is_self") if c in df.columns]
		return df[cols]

	def _write_sql_file(self, users_df: pd.DataFrame | None, reddit_df: pd.DataFrame | None):
		lines = []
		# Header: create tables
		lines.append("-- Generated upsert SQL\n")

		lines.append("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, username TEXT, email TEXT, phone TEXT, website TEXT, address TEXT, company_name TEXT, company_catchphrase TEXT, company_bs TEXT, updated_at TIMESTAMPTZ DEFAULT now());\n")

		lines.append("CREATE TABLE IF NOT EXISTS reddit_posts (id TEXT PRIMARY KEY, title TEXT, author TEXT, score INTEGER, num_comments INTEGER, created_utc TIMESTAMPTZ, selftext TEXT, subreddit TEXT, upvote_ratio NUMERIC, is_self BOOLEAN, updated_at TIMESTAMPTZ DEFAULT now());\n")

		if users_df is not None and not users_df.empty:
			lines.append("-- Upserts for users\n")
			for _, row in users_df.iterrows():
				id_lit = self._sanitize_sql_literal(row.get("id"))
				vals = [self._sanitize_sql_literal(row.get(c)) for c in ["name", "username", "email", "phone", "website", "address", "company_name", "company_catchPhrase", "company_bs"]]
				cols = ["name", "username", "email", "phone", "website", "address", "company_name", "company_catchPhrase", "company_bs"]
				cols_sql = ", ".join(cols)
				vals_sql = ", ".join(vals)
				update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols])
				stmt = f"INSERT INTO users (id, {cols_sql}, updated_at) VALUES ({id_lit}, {vals_sql}, now()) ON CONFLICT (id) DO UPDATE SET {update_sql}, updated_at = now();"
				lines.append(stmt)

		if reddit_df is not None and not reddit_df.empty:
			lines.append("-- Upserts for reddit_posts\n")
			for _, row in reddit_df.iterrows():
				id_lit = self._sanitize_sql_literal(row.get("id"))
				vals = [self._sanitize_sql_literal(row.get(c)) for c in ["title", "author", "score", "num_comments", "created_utc", "selftext", "subreddit", "upvote_ratio", "is_self"]]
				cols = ["title", "author", "score", "num_comments", "created_utc", "selftext", "subreddit", "upvote_ratio", "is_self"]
				cols_sql = ", ".join(cols)
				vals_sql = ", ".join(vals)
				update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols])
				stmt = f"INSERT INTO reddit_posts (id, {cols_sql}, updated_at) VALUES ({id_lit}, {vals_sql}, now()) ON CONFLICT (id) DO UPDATE SET {update_sql}, updated_at = now();"
				lines.append(stmt)

		# Write file
		self.sql_out.parent.mkdir(parents=True, exist_ok=True)
		with open(self.sql_out, "w", encoding="utf-8") as f:
			f.write("\n".join(lines))

		logger.info("Wrote SQL upsert file to %s", self.sql_out)

	def run_pipeline(self):
		users_df = None
		reddit_df = None

		raw_files = list(self.raw_dir.glob("*.json"))
		for p in raw_files:
			name = p.stem
			try:
				df = self._load_json_records(p)
			except Exception:
				logger.exception("failed to load %s", p)
				continue

			# dataset-specific
			if "jsonplaceholder" in name or "json_placeholder" in name or "users" in name:
				users_df = self._process_json_placeholder(df)
			elif "reddit" in name:
				reddit_df = self._process_reddit(df)

		# Write SQL file with upserts
		self._write_sql_file(users_df, reddit_df)
		return {"users": users_df, "reddit": reddit_df}


if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO)
	p = ETLPipeline()
	p.run_pipeline()

