# pip freeze > requirements.txt
python3 -m venv hackthon
source hackthon/bin/activate
pip -r install requirements.txt
# Before running Divide.py remove DEinput.csv, PLinput.csv,  ATinput.csv, NLinput.csv
# First extract the four data sets using the program Divide.py

# second run GETRIDOFBARBERS_DEDATASET.py GETRIDOFBARBERS_ATDATASET.py GETRIDOFBARBERS_NLDATASET.py GETRIDOFBARBERS_NLDATASET.py
# Fianlly run the four UPDATE..DATA.py
you have to use mpi -np (the number of your cpus) python3 GETRIDOFBARBERS_DEDATASET.py
and the four UPDATE..DATA.py
# we tryed to use ParallelProbabilityDE.py etc.. to inspect html code but apart from hotels didn't work always well. That is why we asked llama 3.1 to help us using llama-server and sending  a compressed html with
your questions. The final program is SerialTestAI4.py
We used two tesla P40 for a total of roughly 42 gb for the 8 billions parameters version.
Enjoy our solution!
# Web Crawler and Analysis Script

## Overview

This script is designed to crawl a list of URLs, analyze the content for eCommerce and social media presence, and save the results to a CSV file. The script uses an asynchronous web crawler to fetch web content and then processes the content to determine if it is an eCommerce site and to extract social media handles. The results are saved in a structured format for further analysis.

## Features

- **URL Crawling**: Asynchronously crawls a list of URLs to fetch web content.
- **eCommerce Detection**: Analyzes the content to determine if the site is an eCommerce platform.
- **Social Media Handle Extraction**: Extracts social media handles (Twitter, YouTube, TikTok, LinkedIn) from the content.
- **GPU Usage Monitoring**: Checks GPU usage before processing to ensure resources are available.
- **Progress Bar**: Provides a progress bar to monitor the processing of URLs.

## Dependencies

- Python 3.7+
- `asyncio`
- `crawl4ai`
- `csv`
- `re`
- `logging`
- `requests`
- `subprocess`
- `tqdm`

## Installation

1. **Clone the Repository**:

   
   https://github.com/freesoftwdreamer/Web-Intelligence/Web-Intelligence.git
   cd Web-Intelligence-
