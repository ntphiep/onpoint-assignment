
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    email TEXT,
    phone TEXT,
    website TEXT,
    address TEXT,
    company_name TEXT,
    company_catchphrase TEXT,
    company_bs TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 2) Reddit posts table
CREATE TABLE IF NOT EXISTS reddit_posts (
    id TEXT PRIMARY KEY,
    title TEXT,
    author TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TIMESTAMPTZ,
    selftext TEXT,
    subreddit TEXT,
    upvote_ratio NUMERIC,
    is_self BOOLEAN,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- =============================
-- Usage notes / template for importing CSVs and performing upsert
-- Replace the file paths below with absolute paths if running psql locally on the DB host.
-- Example: psql -h <host> -U <user> -d <db> -f insert_upsert.sql
-- The script uses temporary staging tables and COPY FROM CSV HEADER.
-- =============================

-- Import and upsert users
BEGIN;

-- create temporary staging table
CREATE TEMP TABLE tmp_users (LIKE users INCLUDING DEFAULTS) ON COMMIT DROP;

-- Adjust the path below to the CSV produced by the pipeline
-- Windows example: 'C:/path/to/repo/data/outputs/csv/json_placeholder_users.csv'
-- PostgreSQL server must have permission to read the file when using COPY from server-side path.
-- If you can't use server-side COPY, use psql's \copy from the client instead.
COPY tmp_users (id,name,username,email,phone,website,address,company_name,company_catchphrase,company_bs)
FROM 'data/outputs/csv/json_placeholder_users.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Upsert into target table
INSERT INTO users (id,name,username,email,phone,website,address,company_name,company_catchphrase,company_bs,updated_at)
SELECT id,
       NULLIF(name,'')::text,
       NULLIF(username,'')::text,
       NULLIF(email,'')::text,
       NULLIF(phone,'')::text,
       NULLIF(website,'')::text,
       NULLIF(address,'')::text,
       NULLIF(company_name,'')::text,
       NULLIF(company_catchphrase,'')::text,
       NULLIF(company_bs,'')::text,
       now()
FROM tmp_users
ON CONFLICT (id) DO UPDATE
  SET name = EXCLUDED.name,
      username = EXCLUDED.username,
      email = EXCLUDED.email,
      phone = EXCLUDED.phone,
      website = EXCLUDED.website,
      address = EXCLUDED.address,
      company_name = EXCLUDED.company_name,
      company_catchphrase = EXCLUDED.company_catchphrase,
      company_bs = EXCLUDED.company_bs,
      updated_at = now();

COMMIT;


BEGIN;

CREATE TEMP TABLE tmp_reddit_posts (LIKE reddit_posts INCLUDING DEFAULTS) ON COMMIT DROP;

-- Adjust CSV path as needed
COPY tmp_reddit_posts (id,title,author,score,num_comments,created_utc,selftext,subreddit,upvote_ratio,is_self)
FROM 'data/outputs/csv/reddit_python_posts.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Convert created_utc to timestamp; the CSV stores 'YYYY-MM-DD HH:MM:SS'
INSERT INTO reddit_posts (id,title,author,score,num_comments,created_utc,selftext,subreddit,upvote_ratio,is_self,updated_at)
SELECT id,
       NULLIF(title,'')::text,
       NULLIF(author,'')::text,
       NULLIF(score,'')::integer,
       NULLIF(num_comments,'')::integer,
       -- cast text to timestamptz (assumes timestamps are in UTC). If not, adjust accordingly.
       NULLIF(created_utc,'')::timestamp AT TIME ZONE 'UTC',
       NULLIF(selftext,'')::text,
       NULLIF(subreddit,'')::text,
       NULLIF(upvote_ratio,'')::numeric,
       CASE WHEN lower(NULLIF(is_self,'')) IN ('true','t','1') THEN true ELSE false END,
       now()
FROM tmp_reddit_posts
ON CONFLICT (id) DO UPDATE
  SET title = EXCLUDED.title,
      author = EXCLUDED.author,
      score = EXCLUDED.score,
      num_comments = EXCLUDED.num_comments,
      created_utc = EXCLUDED.created_utc,
      selftext = EXCLUDED.selftext,
      subreddit = EXCLUDED.subreddit,
      upvote_ratio = EXCLUDED.upvote_ratio,
      is_self = EXCLUDED.is_self,
      updated_at = now();

COMMIT;

-- End of script
