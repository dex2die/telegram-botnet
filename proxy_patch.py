import asyncio
import aiohttp
import time
import re
import logging
import random
from typing import Optional, AsyncGenerator

log = logging.getLogger("tg.proxy")

_PROXY_IP_PORT_RE = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}:\d{2,5}")

PROXY_VALIDATE_URL         = "http://httpbin.org/ip"   # http быстрее для проверки
PROXY_VALIDATE_TIMEOUT_SEC = 6
PROXY_VALIDATE_CONCURRENCY = 80
PROXY_MIN_WORKING          = 5   # если меньше — принудительный refresh


def _extract_proxies(text: str) -> list[str]:
    return [f"http://{m}" for m in _PROXY_IP_PORT_RE.findall(text or "")]


async def _fetch_source(session: aiohttp.ClientSession, url: str) -> list[str]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            return _extract_proxies(await r.text())
    except Exception as e:
        log.debug(f"Proxy source failed [{url}]: {e}")
        return []


async def _check_one(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession,
                     proxy: str) -> Optional[str]:
    async with semaphore:
        try:
            async with session.get(
                PROXY_VALIDATE_URL,
                proxy=proxy,
                timeout=aiohttp.ClientTimeout(total=PROXY_VALIDATE_TIMEOUT_SEC),
                allow_redirects=False,
            ) as r:
                if r.status in (200, 301, 302):
                    return proxy
        except Exception:
            pass
    return None


async def validate_proxies(proxies: list[str]) -> list[str]:
    """Параллельная валидация. Возвращает только рабочие прокси."""
    if not proxies:
        return []
    sem = asyncio.Semaphore(PROXY_VALIDATE_CONCURRENCY)
    conn = aiohttp.TCPConnector(ssl=False, limit=PROXY_VALIDATE_CONCURRENCY + 10)
    try:
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [asyncio.create_task(_check_one(sem, session, p)) for p in proxies]
            results = await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await conn.close()
    working = [r for r in results if isinstance(r, str)]
    log.info(f"Validated {len(proxies)} → {len(working)} working proxies")
    return working


class FastProxyPool:
    """
    Оптимизированный пул прокси с предварительной валидацией.

    Отличия от оригинального ProxyPool:
    • Валидирует все прокси параллельно (80 одновременно) — нерабочие не попадают в пул
    • Хранит пул ТОЛЬКО рабочих прокси → get_proxy() = O(1), без итераций
    • mark_good() перемещает надёжные прокси в начало очереди
    • Фоновое обновление — не блокирует генерацию
    • max_attempts снижен до 2 (пул чистый, 2 попытки достаточно)
    • Автоматический refresh если рабочих < PROXY_MIN_WORKING
    """

    def __init__(self, mode: str, sources: list[str], refresh_sec: int, max_attempts: int):
        self.mode        = mode if mode in ("off", "auto", "force") else "auto"
        self.sources     = [s for s in (sources or []) if s]
        self.refresh_sec = max(60, int(refresh_sec or 900))
        self.max_attempts = 2  # принудительно — т.к. пул чистый

        self._working:      list[str] = []
        self._bad:          set[str]  = set()
        self._idx:          int       = 0
        self._last_refresh: float     = 0.0
        self._refreshing:   bool      = False
        self._lock                    = asyncio.Lock()

    def enabled(self) -> bool:
        return self.mode != "off"

    # ── Публичный API ──────────────────────────────────────────────────────────

    async def initialize(self) -> None:
        """Вызвать при старте бота — запустит первый refresh в фоне."""
        if self.mode == "off" or not self.sources:
            return
        asyncio.create_task(self._do_refresh())

    async def get_proxy(self) -> Optional[str]:
        """Мгновенно вернуть следующий рабочий прокси."""
        if self.mode == "off":
            return None
        await self._trigger_refresh_if_needed()
        async with self._lock:
            if not self._working:
                if self.mode == "force":
                    raise RuntimeError("Proxy mode=force but no working proxies available")
                return None
            proxy = self._working[self._idx % len(self._working)]
            self._idx = (self._idx + 1) % len(self._working)
            return proxy

    def mark_bad(self, proxy: Optional[str]) -> None:
        if not proxy:
            return
        self._bad.add(proxy)
        try:
            self._working.remove(proxy)
        except ValueError:
            pass

    def mark_good(self, proxy: Optional[str]) -> None:
        """Поставить надёжный прокси в начало ротации."""
        if not proxy or proxy in self._bad:
            return
        if proxy in self._working:
            self._working.remove(proxy)
        self._working.insert(0, proxy)

    async def iter_proxies(self, attempts: Optional[int] = None) -> AsyncGenerator[Optional[str], None]:
        if self.mode == "off":
            yield None
            return
        max_a = attempts if attempts is not None else self.max_attempts
        used = 0
        while used < max_a:
            proxy = await self.get_proxy()
            if proxy:
                yield proxy
                used += 1
            else:
                break
        if self.mode == "auto":
            yield None  # финальная попытка без прокси

    @property
    def stats(self) -> dict:
        return {
            "working": len(self._working),
            "bad": len(self._bad),
            "mode": self.mode,
            "age_sec": round(time.monotonic() - self._last_refresh),
        }

    # ── Внутренние методы ──────────────────────────────────────────────────────

    async def _trigger_refresh_if_needed(self) -> None:
        now = time.monotonic()
        low_proxies = len(self._working) < PROXY_MIN_WORKING
        stale       = (now - self._last_refresh) >= self.refresh_sec
        if (low_proxies or stale) and not self._refreshing:
            asyncio.create_task(self._do_refresh())

    async def _do_refresh(self) -> None:
        if self._refreshing:
            return
        self._refreshing = True
        try:
            raw = await self._fetch_all()
            if not raw:
                log.warning("Proxy refresh: no proxies fetched")
                return
            t0 = time.monotonic()
            working = await validate_proxies(raw)
            elapsed = time.monotonic() - t0
            log.info(f"Proxy refresh done in {elapsed:.1f}s — {len(working)} working")
            async with self._lock:
                # Убираем уже забаненные
                self._working = [p for p in working if p not in self._bad]
                random.shuffle(self._working)
                self._idx = 0
                self._last_refresh = time.monotonic()
        except Exception as e:
            log.error(f"Proxy refresh error: {e}")
        finally:
            self._refreshing = False

    async def _fetch_all(self) -> list[str]:
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as s:
            tasks   = [_fetch_source(s, url) for url in self.sources]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        seen, out = set(), []
        for res in results:
            if not isinstance(res, list):
                continue
            for p in res:
                if p not in seen and p not in self._bad:
                    seen.add(p)
                    out.append(p)
        log.info(f"Fetched {len(out)} unique proxies from {len(self.sources)} sources")
        return out


# ══════════════════════════════════════════════════════════════════════════════
# Async OSS Upload — заменяет синхронный requests в asyncio.to_thread
# ══════════════════════════════════════════════════════════════════════════════

def _build_oss_multipart(sign, image_bytes: bytes, content_type: str) -> tuple[bytes, str]:
    """
    Вручную строим multipart/form-data — точно как requests.
    aiohttp.FormData добавляет Content-Type к текстовым полям,
    что ломает парсер OSS → MalformedPOSTRequest.
    OSS требует: текстовые поля БЕЗ Content-Type, файл — с Content-Type.
    """
    import uuid
    boundary = f"----FormBoundary{uuid.uuid4().hex}"

    def text_part(name: str, value: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n'
            f"\r\n"
            f"{value}\r\n"
        ).encode("utf-8")

    parts: list[bytes] = []
    # Порядок важен: key — первый
    parts.append(text_part("key",                    sign.filename))
    parts.append(text_part("OSSAccessKeyId",         sign.access_id))
    parts.append(text_part("policy",                 sign.policy))
    parts.append(text_part("Signature",              sign.signature))
    parts.append(text_part("Content-Type",           content_type))
    parts.append(text_part("x-oss-forbid-overwrite", "true"))
    parts.append(text_part("x:type",                 sign.file_type))
    parts.append(text_part("x:user",                 sign.user_id))
    parts.append(text_part("x:region",               sign.region))
    if sign.callback:
        parts.append(text_part("callback", sign.callback))

    # Файл — с Content-Type
    file_header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{sign.filename}"\r\n'
        f"Content-Type: {content_type}\r\n"
        f"\r\n"
    ).encode("utf-8")
    parts.append(file_header + image_bytes + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))

    body = b"".join(parts)
    ctype = f"multipart/form-data; boundary={boundary}"
    return body, ctype


async def upload_to_oss_async(
    sign,
    image_bytes: bytes,
    content_type: str = "image/jpeg",
    proxy_pool: Optional["FastProxyPool"] = None,
) -> None:
    """
    Асинхронная загрузка в Alibaba Cloud OSS.
    Multipart строится вручную — aiohttp.FormData добавляет Content-Type
    к текстовым полям, что ломает парсер OSS (MalformedPOSTRequest).
    """
    last_err: Optional[Exception] = None

    async def _attempt(proxy: Optional[str]) -> bool:
        nonlocal last_err
        body, ctype = _build_oss_multipart(sign, image_bytes, content_type)
        connector = aiohttp.TCPConnector(ssl=False)
        try:
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=90),
            ) as s:
                async with s.post(
                    sign.host,
                    data=body,
                    headers={"Content-Type": ctype},
                    proxy=proxy,
                ) as r:
                    if r.status in (200, 204):
                        if proxy_pool and proxy:
                            proxy_pool.mark_good(proxy)
                        return True
                    resp_body = await r.text()
                    raise RuntimeError(f"OSS {r.status}: {resp_body[:300]}")
        except Exception as e:
            last_err = e
            if proxy_pool and proxy:
                proxy_pool.mark_bad(proxy)
            return False
        finally:
            await connector.close()

    if proxy_pool and proxy_pool.enabled():
        async for proxy in proxy_pool.iter_proxies():
            if await _attempt(proxy):
                return
    else:
        if await _attempt(None):
            return

    raise last_err or RuntimeError("OSS upload failed after all attempts")
