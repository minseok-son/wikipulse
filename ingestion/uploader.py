import requests
from bs4 import BeautifulSoup
import boto3
from urllib.parse import urljoin
import re

class WikiIngestor:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.base_url = "https://dumps.wikimedia.org/other/pageviews/"

    def get_file_links(self, year, month):
        """Scrapes the Wikimedia page for .gz file links."""
        folder_url = urljoin(self.base_url, f"{year}/{year}-{month:02d}/")
        print(f"Scraping links from: {folder_url}")
        
        response = requests.get(folder_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pattern = re.compile(r'pageviews-\d{8}-\d{6}\.gz')
        links = [urljoin(folder_url, a['href']) for a in soup.find_all('a', href=pattern)]
        
        return sorted(list(set(links)))

    def stream_to_s3(self, file_url):
        """Streams a file from a URL directly to S3 without saving locally."""
        filename = file_url.split('/')[-1]
        
        # Parse date from filename for Hive-style partitioning
        # Example: pageviews-20260301-010000.gz
        date_part = filename.split('-')[1] # 20260301
        year, month, day = date_part[:4], date_part[4:6], date_part[6:8]
        
        s3_key = f"bronze/year={year}/month={month}/day={day}/{filename}"
        
        print(f"Streaming {filename} to s3://{self.bucket}/{s3_key}...")
        
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            self.s3.upload_fileobj(r.raw, self.bucket, s3_key)

if __name__ == "__main__":
    # CONFIGURATION
    BUCKET = "wikipulse"
    YEAR = 2026
    MONTH = 3
    
    ingestor = WikiIngestor(BUCKET)
    
    all_links = ingestor.get_file_links(YEAR, MONTH)
    print(f"Found {len(all_links)} files to upload.")

    for link in all_links:
        try:
            ingestor.stream_to_s3(link)
        except Exception as e:
            print(f"Failed to upload {link}: {e}")