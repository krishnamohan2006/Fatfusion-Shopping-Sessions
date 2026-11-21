# Fatfusion · Shopping Sessions

The container reads the mounted CSV files and writes the two daily tables to
`/output/user_daily/` and `/output/campaign_daily/`. It runs one C++17 executable.
The default Dockerfile packages the bundled linux/amd64 executable on a pinned
Debian Bookworm base and runs as UID 1000. Only the base image needs to be pulled.
The source-build Dockerfile compiles the same pipeline locally.

## Submit from PowerShell

1. Create an empty public GitHub repository and copy its HTTPS URL.
2. Open PowerShell in this submission repository and run the following.

```powershell
$repoUrl = Read-Host "Paste the empty public repository HTTPS URL"
git remote add origin $repoUrl
git push -u origin main
git rev-parse HEAD
```

3. On the Shopping Sessions track page, paste the public repository URL and the
   full 40-character commit printed above. Click **Submit**. Keep the repository
   public and the commit accessible until the organizers publish results.

## Build and run

From this directory:

```sh
docker build --platform linux/amd64 -t shopping-fatfusion .
```

The binary was built from the included C++17 source using the pinned Bookworm
compiler image in `Dockerfile.source`. Its SHA-256 is
`96608d584a57016d08916a51be0c01561e33616eeaf46608b255c62f54eecc62`.
To rebuild the entire image from C++ source:

```sh
docker build --platform linux/amd64 -f Dockerfile.source -t shopping-fatfusion-source .
```

With the organizer's extracted starter, run its fixture checker on Linux or WSL.
Set `STARTER` to the absolute path of the extracted starter directory.

```sh
export IMAGE=shopping-fatfusion
python3 "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- sh "$STARTER/scripts/docker_run.sh"
```

For the official development check, build the supplied Rust generator and run the
following with its executable on PATH. Set `ANSWER` to the directory containing the
two extracted answer CSVs. Use a fresh output directory.

```sh
generate --seed 1 --events 1000000 --clicks 250000 --output dev
sh "$STARTER/scripts/docker_run.sh" dev out
python3 "$STARTER/scripts/compare.py" out "$ANSWER"
```

The starter's shell wrapper accepts `CPUS` and `MEM` overrides for smaller local
machines. The comparison scripts require Python with Polars on the host.

## Implementation

- Union-find resolves link chains and tracks the byte-wise minimum alias independently
  of the union root. Unlinked IDs retain their original bytes.
- The parser scans newline-delimited, memory-mapped chunks and validates every field's
  quoting, including fields whose values the business rules ignore. It preserves CR,
  spaces, and case. Timestamps convert directly to integer UTC milliseconds.
- A first pass builds a repeat filter for event IDs. Every repeated ID goes to exact
  byte-wise deduplication, including rows whose payload is invalid. A filter collision
  costs extra work. The smallest ingestion timestamp determines the surviving row.
- Workers group surviving events and eligible clicks by canonical person, sort each
  timeline, then advance two cursors through the seven-day click window. Campaign
  reference counts handle repeated clicks and count each campaign once per session.
- Short IDs are stored inline inside event and click records so the aggregation pass
  can access them without repeatedly fetching input pages. Longer IDs retain their full bytes.
- Each worker writes typed user rows directly to Parquet. A small final reduction
  merges campaign-day totals. The writer emits flat, uncompressed Parquet V1 pages
  with UTF-8, DATE, signed integers, and definition levels for nullable categories.
  Execution requires no network or scratch intermediates.
