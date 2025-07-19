# Civilica Conference Scraper

A parallel web scraper for extracting conference paper data from [Civilica.com](https://civilica.com).

## 📋 Command-Line Arguments

| Argument | Description | Default Value | Type |
|----------|-------------|---------------|------|
| `--input` | Path to input CSV file | `conferences_merged_full.csv` | string |
| `--output` | Path to output CSV file | `civilica_parallel_output.csv` | string |
| `--failed` | Path to failed URLs log | `failed_urls.csv` | string |
| `--filtered` | Path to filtered conference IDs | `filtered_conference_ids.csv` | string |
| `--driver` | Path to Edge WebDriver executable | `C:\Users\Ali\msedgedriver.exe` | string |
| `--start` | Starting index for conference IDs | `0` | int |
| `--end` | Ending index for conference IDs | `None` (process all) | int |
| `--workers` | Number of parallel workers | `8` | int |
| `--headless` | Run browser in headless mode | `False` | flag |
| `--no-parallel` | Disable parallel processing | `False` | flag |
| `--timeout` | Page load timeout (seconds) | `12` | int |
| `--retries` | Max retries for failed requests | `2` | int |
| `--save-every` | Save partial results after N rows | `100` | int |
| `--min-delay` | Min delay between requests (s) | `0.1` | float |
| `--max-delay` | Max delay between requests (s) | `0.5` | float |

## 🚀 Usage Examples

### Basic Run (Defaults)
```bash
python scraper.py