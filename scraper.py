import csv
import pickle
import time

import requests
from bs4 import BeautifulSoup

from logger import log

FILEPATHS = ["./data/train.csv", "./data/test.csv"]
OUTPUTS_PATH = "./output/scraped_data.pkl"
NEWS_ID_COL = 3
SAVE_FREQ = 10
INTERVAL = 0.5
URL_NEWS_TOPIC = "http://news.livedoor.com/topics/detail/{news_id}/"
URL_NEWS_BODY = "http://news.livedoor.com/article/detail/{news_id}/"
SELECTOR_TITLE = "#article-body > article > header > h1"
SELECTOR_BODY = "#article-body > article > div.articleBody > span"
SELECTOR_SUMMARY = (
    "#main > div > article > div.topicsBody > div.summaryBox > div > ul > li"
)
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}


def load_news_id(filepath: str):
    log.debug(f"loading {filepath}")
    with open(filepath, encoding="utf8", newline="") as f:
        csvreader = csv.reader(f)
        news_id_list = [row[NEWS_ID_COL] for row in csvreader]
    log.debug(f"{len(news_id_list)} news_id is loaded!")
    return news_id_list


def scrape_article(news_id: str):
    output = {"news_id": news_id, "title": None, "body": None, "summary": None}

    try:
        # --- 本文を取得
        url = URL_NEWS_BODY.format(news_id=news_id)
        resp = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(resp.content, "lxml")
        title = soup.select_one(SELECTOR_TITLE)
        body = soup.select_one(SELECTOR_BODY)
        output["title"] = title.get_text()
        output["body"] = body.get_text()
    except Exception as e:
        log.error(f"Error in BODY: {e}")

    try:
        # 要約を取得
        url = URL_NEWS_TOPIC.format(news_id=news_id)
        resp = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(resp.content, "lxml")
        summary = soup.select(SELECTOR_SUMMARY)
        output["summary"] = [sentence.get_text() for sentence in summary]
    except Exception as e:
        log.error(f"Error in summary: {e}")

    return output


def save2pkl(outputs: list):
    log.debug(f"Saving outputs: len(outputs) = {len(outputs)}")
    log.debug(f"{outputs[-20:]}")
    with open(OUTPUTS_PATH, "wb") as f:
        pickle.dump(outputs, f)


def run():
    news_id_list = []
    for filepath in FILEPATHS:
        news_id_list.extend(load_news_id(filepath))

    outputs = []
    for i, news_id in enumerate(news_id_list):
        log.debug(f"{i}/{len(news_id_list)}: news_id={news_id}")
        outputs.append(scrape_article(news_id))

        if i % SAVE_FREQ == 0:
            save2pkl(outputs)
        time.sleep(INTERVAL)
    save2pkl(outputs)


if __name__ == "__main__":
    run()
