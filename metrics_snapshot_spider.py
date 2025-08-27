import os
import sqlite3
from datetime import datetime
from typing import List, Tuple
import scrapy

# ── DB 設定（環境変数で上書き可） ─────────────────────────────
DB_PATH = os.getenv("MARKET_DB_PATH", os.path.abspath("./market_data.db"))

class MetricsSnapshotSpider(scrapy.Spider):
    name = "metrics_snapshot"

    # 控えめな polite 設定（必要に応じて起動時 -s で上書き可）
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,

        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 3,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 0.5,

        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429],

        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_DIR": "httpcache",
        "DOWNLOAD_TIMEOUT": 25,

        "USER_AGENT": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "DEFAULT_REQUEST_HEADERS": {"Referer": "https://example.com"},
        "LOG_LEVEL": "INFO",
    }

    allowed_domains = ["example.com"]                         # 固有名は出さない
    start_urls = ["https://example.com/markets/list/"]        # ダミーの一覧URL

    def __init__(self, target_date=None, batch_size=50, *args, **kwargs):
        """
        :param target_date: YYYYMMDD（必須：検証は start_requests で実施）
        :param batch_size: バッチ保存件数（既定: 50）
        """
        super().__init__(*args, **kwargs)
        self.target_date = target_date
        try:
            self.batch_size = int(batch_size) if batch_size else 50
            if self.batch_size <= 0:
                self.batch_size = 50
        except Exception:
            self.batch_size = 50

        # DB 準備
        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()
        self.cur.execute("PRAGMA journal_mode=WAL;")
        self.cur.execute("PRAGMA synchronous=NORMAL;")
        self.conn.commit()
        self._init_db()

        self._buf: List[Tuple] = []
        self._queued = 0
        self._parsed = 0

    # ── テーブル作成（ダミー構造の指標スナップショット） ────────────────
    def _init_db(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS metrics_snapshot (
                target_date     TEXT,
                code            TEXT,
                sector          TEXT,
                name            TEXT,
                price_text      TEXT,
                pe              TEXT,
                dividend_yield  TEXT,
                pb              TEXT,
                roe             TEXT,
                earning_yield   TEXT,
                UNIQUE(code, target_date)
            )
        """)
        self.conn.commit()

    # ── 起動時バリデーション ─────────────────────────────────
    def start_requests(self):
        if not self.target_date:
            raise ValueError("実行時に -a target_date=YYYYMMDD の形式で日付を指定してください")
        datetime.strptime(self.target_date, "%Y%m%d")
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_list, dont_filter=True)

    # ── 一覧ページのパース（ダミーの一般的テーブル構造） ──────────────
    def parse_list(self, response: scrapy.http.Response):
        # 例：テーブルに class="item-list" が付与されている想定
        rows = response.xpath('//table[contains(@class,"item-list")]//tr')
        for row in rows:
            # 例：1列目=コード、2列目=名称、2列目に詳細リンクがある想定
            code = row.xpath('./td[1]//text()').get()
            name = row.xpath('./td[2]//text()').get()
            detail_href = row.xpath('./td[2]//a/@href').get()

            if code and name and detail_href:
                self._queued += 1
                detail_url = response.urljoin(detail_href)
                yield scrapy.Request(
                    detail_url,
                    callback=self.parse_detail,
                    meta={"code": code, "name": name},
                    dont_filter=True
                )

        # Next ページ（一般的なUI文言を想定）
        next_page = response.xpath('//a[normalize-space(text())="Next"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_list)

    # ── 詳細ページのパース（ダミーXPathで簡易メタ取得） ───────────────
    def parse_detail(self, response: scrapy.http.Response):
        code = response.meta["code"]
        name = response.meta["name"]

        # それっぽい位置にある想定のダミーXPath
        sector        = self._get(response, '//*[@id="main"]//span[@class="category"]/text()')
        price_text    = self._get(response, '//*[@id="main"]//div[@class="price"]/text()')
        pe            = self._get(response, '//*[@id="metrics"]//li[@data-key="pe"]/span/text()')
        dividend_yld  = self._with_pct(self._get(response, '//*[@id="metrics"]//li[@data-key="div_yld"]/span/text()'))
        pb            = self._get(response, '//*[@id="metrics"]//li[@data-key="pb"]/span/text()')
        roe           = self._with_pct(self._get(response, '//*[@id="metrics"]//li[@data-key="roe"]/span/text()'))
        earning_yield = self._with_pct(self._get(response, '//*[@id="metrics"]//li[@data-key="earn_yld"]/span/text()'))

        self._buf.append((
            self.target_date, code, sector, name, price_text,
            pe, dividend_yld, pb, roe, earning_yield
        ))
        self._parsed += 1

        if len(self._buf) >= self.batch_size:
            self._flush()

        if self._parsed % 50 == 0:
            self.logger.info(f"[PARSE] parsed={self._parsed} queued≈{self._queued}")

    # ── ユーティリティ ───────────────────────────────────────
    def _get(self, resp: scrapy.http.Response, xp: str) -> str:
        try:
            v = resp.xpath(xp).get()
            return v.strip() if v else "N/A"
        except Exception:
            return "N/A"

    def _with_pct(self, s: str) -> str:
        s = (s or "").strip()
        if not s or s == "N/A":
            return "N/A"
        return s if s.endswith("%") else s + "%"

    def _flush(self):
        if not self._buf:
            return
        self.cur.executemany("""
            INSERT OR REPLACE INTO metrics_snapshot (
                target_date, code, sector, name, price_text,
                pe, dividend_yield, pb, roe, earning_yield
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, self._buf)
        self.conn.commit()
        self.logger.info(f"[DB] commit {len(self._buf)} rows (REPLACE)")
        self._buf.clear()

    def closed(self, reason):
        try:
            self._flush()
        finally:
            try:
                self.conn.close()
            except Exception:
                pass
        self.logger.info(f"[CLOSE] reason={reason} queued≈{self._queued} parsed={self._parsed}")
