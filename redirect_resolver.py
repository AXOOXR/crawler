import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from typing import Tuple
import os
import sys

def resolve_redirect_worker(url: str,
                             nav_timeout: int = 30_000,
                             post_wait_ms: int = 5000) -> Tuple[str, str]:
    """Worker function: resolves URL redirects using Playwright in a separate thread."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            print(f"Opening → {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
            page.wait_for_load_state("networkidle", timeout=nav_timeout)
        except PlaywrightTimeoutError:
            print(f"Timeout → {url}")
        except Exception as e:
            print(f"Error → {url}: {e}")
        page.wait_for_timeout(post_wait_ms)
        final = page.url
        browser.close()
        return url, final


def main(input_csv: str,
         output_csv: str,
         url_column: str = "website",
         start_index: int = 0,
         end_index: int = None,
         max_workers: int = 8,
         save_every: int = 100):
    
    df = pd.read_csv(input_csv)

    if url_column not in df.columns:
        raise KeyError(f"Column '{url_column}' not found in {input_csv}")

    # Slice the desired range
    sliced_df = df.iloc[start_index:end_index].copy()
    urls = sliced_df[url_column].dropna().tolist()

    results = {}
    completed_count = 0

    # Check if output already exists (for checkpointing/resuming)
    if os.path.exists(output_csv):
        try:
            saved_df = pd.read_csv(output_csv)
            already_done = set(saved_df[url_column])
            print(f"Loaded {len(already_done)} previously saved results.")
        except:
            already_done = set()
    else:
        already_done = set()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(resolve_redirect_worker, url): url
            for url in urls if url not in already_done
        }

        for i, future in enumerate(as_completed(future_to_url), start=1):
            orig_url = future_to_url[future]
            try:
                src, resolved = future.result()
                results[src] = resolved
                print(f"[{i}/{len(future_to_url)}] Resolved: {src} → {resolved}")
            except Exception as e:
                print(f"Error resolving {orig_url}: {e}")
                results[orig_url] = None

            completed_count += 1

            # Save every N completions
            if completed_count % save_every == 0 or completed_count == len(future_to_url):
                # Update Final URL column with results so far
                sliced_df["Final URL"] = sliced_df[url_column].map(
                    lambda x: results.get(x) if x in results else None
                )
                sliced_df.to_csv(output_csv, index=False)
                print(f"✅ Checkpoint saved after {completed_count} resolved.")

    # Final save after all
    sliced_df["Final URL"] = sliced_df[url_column].map(
        lambda x: results.get(x) if x in results else None
    )
    sliced_df.to_csv(output_csv, index=False)
    print(f"\n✅ All done. Final results saved to: {output_csv}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Command-line arguments
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        start_idx = int(sys.argv[3]) if len(sys.argv) > 3 else 0
        end_idx = int(sys.argv[4]) if len(sys.argv) > 4 else None
        workers = int(sys.argv[5]) if len(sys.argv) > 5 else 8
        
        main(
            input_csv=input_file,
            output_csv=output_file,
            start_index=start_idx,
            end_index=end_idx,
            max_workers=workers
        )
    else:
        # Default values if no args provided
        INPUT_FILE = "conferences_merged_full.csv"
        OUTPUT_FILE = "final_links_part9.csv"
        main(
            input_csv=INPUT_FILE,
            output_csv=OUTPUT_FILE,
            url_column="website",
            start_index=8000,
            end_index=9991,
            max_workers=12,
            save_every=100
        )