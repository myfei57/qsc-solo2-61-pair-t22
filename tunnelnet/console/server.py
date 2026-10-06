"""WSGI application that serves the control console."""

from __future__ import annotations

import os
from typing import Callable, Iterable

from tunnelnet.sim import SimulationManager
from tunnelnet.sim.gate import WriteGate
from tunnelnet.store import Store

from . import handlers
from .http import Request, RequestError, Response, Router, build_request, error_response
from .metrics import Metrics
from .pages import load_page, page_file, root_redirect
from .routes import MUTATING_ROUTES, register_routes
from .runtime import Runtime
from .seed import seed_defaults


class Server:
    """Owns the runtime state and dispatches console requests."""

    def __init__(self, store: Store, web_dir: str = "web") -> None:
        self.runtime = Runtime(store)
        seed_defaults(self.runtime)
        self.web_dir = web_dir
        self.router = Router()
        self.metrics = Metrics()
        self.write_gate = WriteGate()
        rounds_root = os.path.join(os.path.dirname(store.root), "sim-rounds")
        self.simulations = SimulationManager(self.runtime, rounds_root, self.write_gate)
        register_routes(self.router, handlers)

    def respond(self, request: Request) -> Response:
        response = self._resolve(request)
        self.metrics.record(request.method, request.path, response.status)
        return response

    def _resolve(self, request: Request) -> Response:
        if request.method == "GET":
            filename = page_file(request.path)
            if filename is not None:
                return load_page(self.web_dir, filename)
            if request.path == "/":
                return root_redirect()
        return self._route(request)

    def _route(self, request: Request) -> Response:
        handler = self.router.match(request.method, request.path)
        if handler is None:
            if self.router.allows(request.path):
                return error_response(
                    405, f"method {request.method} not allowed for {request.path}"
                )
            return error_response(404, f"no route for {request.path}")
        # Serialize online mutations against snapshot capture. The simulation
        # endpoints themselves only take a read snapshot and run on a clone, so
        # they are not treated as online writers.
        scope = (
            self.write_gate.write()
            if request.method == "POST" and (request.method, request.path) in MUTATING_ROUTES
            else _NullGate()
        )
        try:
            with scope:
                return handler(self, request)
        except RequestError as exc:
            return error_response(exc.status, exc.message)
        except ValueError as exc:
            return error_response(400, str(exc))
        except Exception as exc:  # noqa: BLE001 - last resort for a live console
            return error_response(500, str(exc))

    def dispatch(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        query: dict[str, list[str]] | None = None,
    ) -> Response:
        """Call a route directly, used by the test suite and CLI probes."""

        request = Request(
            method=method.upper(),
            path=path,
            query=dict(query or {}),
            payload=dict(payload or {}),
        )
        return self.respond(request)

    def __call__(
        self, environ: dict[str, object], start_response: Callable[..., object]
    ) -> Iterable[bytes]:
        try:
            response = self.respond(build_request(environ))
        except RequestError as exc:
            response = error_response(exc.status, exc.message)
        status, headers = response.wsgi()
        start_response(status, headers)
        return [response.body]


def create_application(store: Store, web_dir: str = "web") -> Server:
    """Build a ready to serve console for the supplied store."""

    return Server(store, web_dir)


class _NullGate:
    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None
