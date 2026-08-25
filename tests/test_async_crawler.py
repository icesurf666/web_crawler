import asyncio
from time import perf_counter

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from async_crawler import AsyncCrawler
from errors import PermanentError, TransientError


class TestAsyncCrawler:
    @pytest.mark.asyncio
    async def test_fetch_valid_url(self) -> None:
        async def valid_page(_request: web.Request) -> web.Response:
            return web.Response(text="Hello world!")

        app = web.Application()
        app.router.add_get("/page", valid_page)

        server = TestServer(app)
        await server.start_server()

        url = str(server.make_url("/page"))
        crawler = AsyncCrawler(max_concurrent=1)

        try:
            content = await crawler.fetch_url(url)
        finally:
            await crawler.close()
            await server.close()

        assert content == "Hello world!"

    @pytest.mark.asyncio
    async def test_fetch_nonexistent_url(self) -> None:
        async def missing_page(_request: web.Request) -> web.Response:
            return web.Response(status=404, text="Page not found")

        app = web.Application()
        app.router.add_get("/missing", missing_page)

        server = TestServer(app)
        await server.start_server()

        url = str(server.make_url("/missing"))
        crawler = AsyncCrawler(max_concurrent=1)

        try:
            with pytest.raises(PermanentError) as exc_info:
                await crawler.fetch_url(url)
        finally:
            await crawler.close()
            await server.close()

        assert exc_info.value.status == 404
        assert exc_info.value.url == url

    @pytest.mark.asyncio
    async def test_timeout(self) -> None:
        async def slow_page(_request: web.Request) -> web.Response:
            await asyncio.sleep(0.2)
            return web.Response(text="Slow response")

        app = web.Application()
        app.router.add_get("/slow", slow_page)

        server = TestServer(app)
        await server.start_server()

        url = str(server.make_url("/slow"))
        crawler = AsyncCrawler(max_concurrent=1)
        crawler.timeout = aiohttp.ClientTimeout(connect=1, sock_read=0.05)

        try:
            with pytest.raises(TransientError) as exc_info:
                await crawler.fetch_url(url)
        finally:
            await crawler.close()
            await server.close()

        assert exc_info.value.url == url

    @pytest.mark.asyncio
    async def test_parallel_is_faster_than_sequential(self) -> None:
        async def delayed_page(_request: web.Request) -> web.Response:
            await asyncio.sleep(0.1)
            return web.Response(text="Completed")

        app = web.Application()
        app.router.add_get("/delayed", delayed_page)

        server = TestServer(app)
        await server.start_server()

        urls = [str(server.make_url(f"/delayed?id={number}")) for number in range(3)]
        crawler = AsyncCrawler(max_concurrent=3)

        try:
            started = perf_counter()

            for url in urls:
                await crawler.fetch_url(url)

            sequential_time = perf_counter() - started

            started = perf_counter()
            results = await crawler.fetch_urls(urls)
            parallel_time = perf_counter() - started
        finally:
            await crawler.close()
            await server.close()

        assert len(results) == 3
        assert parallel_time < sequential_time
