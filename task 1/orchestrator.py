"""
Static-scaling orchestrator for XMLRPC and Pyro.

Provides round-robin routing across N running backend instances.

XMLRPC port conventions
-----------------------
  Node k  service → 8000 + 2*k
  Node k  filter  → 8001 + 2*k

Before launching the orchestrator, start the backends on different ports:

  XMLRPC_SERVICE_PORT=8000 python xmlrpc/insult_service.py  &
  XMLRPC_SERVICE_PORT=8002 python xmlrpc/insult_service.py  &
  XMLRPC_FILTER_PORT=8001  XMLRPC_SERVICE_PORT=8000 python xmlrpc/insult_filter.py  &
  XMLRPC_FILTER_PORT=8003  XMLRPC_SERVICE_PORT=8002 python xmlrpc/insult_filter.py  &

The orchestrator listens on 9000 (service) or 9001 (filter) by default:

  python orchestrator.py xmlrpc service --nodes 2
  python orchestrator.py xmlrpc filter  --nodes 3

Then point the stress client at the orchestrator:

  python client.py xmlrpc service --all --port 9000
  python client.py xmlrpc filter  --all --port 9001

Pyro multi-node
---------------
Start N Pyro service/filter instances and collect their URIs. Then:

  python orchestrator.py pyro service --nodes 2 \\
      --uris "PYRO:obj@localhost:12345,PYRO:obj@localhost:12346"

The orchestrator writes its URI into settings.json so client.py picks it up automatically.
Alternatively, populate "service_uris" / "filter_uris" arrays in settings.json.

For Redis and RabbitMQ
----------------------
No orchestrator is needed — just start N filter worker processes.
The shared queue distributes work automatically:

  for i in $(seq 1 3); do python redis/insult_filter.py & done
  for i in $(seq 1 3); do python rabbitmq/insult_filter.py & done
"""

import sys
import json
import argparse
import itertools
import os


def run_xmlrpc(target, nodes, port):
    import xmlrpc.client
    from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

    base     = 8000 if target == "service" else 8001
    backends = [xmlrpc.client.ServerProxy(f"http://localhost:{base + 2*k}") for k in range(nodes)]
    rr       = itertools.cycle(range(nodes))

    class _Handler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    if target == "service":
        class Orch:
            def add_insult(self, insult):
                return backends[next(rr)].add_insult(insult)

            def get_insults(self):
                seen, out = set(), []
                for b in backends:
                    for ins in b.get_insults():
                        if ins not in seen:
                            seen.add(ins)
                            out.append(ins)
                return out
    else:
        class Orch:
            def submit_text(self, text):
                return backends[next(rr)].submit_text(text)

            def get_filtered(self):
                out = []
                for b in backends:
                    out.extend(b.get_filtered())
                return out

    with SimpleXMLRPCServer(("localhost", port), requestHandler=_Handler, logRequests=False) as srv:
        srv.register_instance(Orch())
        backend_ports = [base + 2*k for k in range(nodes)]
        print(f"XMLRPC {target} orchestrator — {nodes} node(s) → {backend_ports}, proxy on :{port}")
        srv.serve_forever()


def run_pyro(target, nodes, uri_list, settings_path):
    import Pyro4

    if not uri_list:
        with open(settings_path) as f:
            data = json.load(f)
        key      = "service_uris" if target == "service" else "filter_uris"
        uri_list = data.get(key, [])
        if not uri_list:
            sys.exit(
                f"No URIs found. Either pass --uris or populate '{key}' list in {settings_path}"
            )

    uri_list = [u.strip() for u in uri_list[:nodes]]
    proxies  = [Pyro4.Proxy(u) for u in uri_list]
    rr       = itertools.cycle(range(len(proxies)))

    if target == "service":
        @Pyro4.expose
        class Orch:
            def add_insult(self, insult):
                return proxies[next(rr)].add_insult(insult)

            def get_insults(self):
                seen, out = set(), []
                for p in proxies:
                    for ins in p.get_insults():
                        if ins not in seen:
                            seen.add(ins)
                            out.append(ins)
                return out
    else:
        @Pyro4.expose
        class Orch:
            def submit_text(self, text):
                return proxies[next(rr)].submit_text(text)

            def get_filtered(self):
                out = []
                for p in proxies:
                    out.extend(p.get_filtered())
                return out

    daemon = Pyro4.Daemon()
    uri    = daemon.register(Orch())

    with open(settings_path, "r+") as f:
        data = json.load(f)
        okey = "orchestrator_service_uri" if target == "service" else "orchestrator_filter_uri"
        data[okey] = str(uri)
        f.seek(0)
        json.dump(data, f, indent=4)
        f.truncate()

    print(f"Pyro {target} orchestrator — {len(proxies)} node(s): {uri}")
    print(f"  URI written to {settings_path!r} as '{okey}'")
    daemon.requestLoop()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("middleware", choices=["xmlrpc", "pyro"])
    parser.add_argument("target",     choices=["service", "filter"])
    parser.add_argument("--nodes",    type=int, default=1,
                        help="Number of backend instances to route between (default 1)")
    parser.add_argument("--port",     type=int, default=None,
                        help="Orchestrator listen port (default 9000/service, 9001/filter)")
    parser.add_argument("--uris",     default=None,
                        help="Comma-separated Pyro4 backend URIs (Pyro only)")
    parser.add_argument("--settings", default=None,
                        help="Path to pyro/settings.json (Pyro only)")
    args = parser.parse_args()

    if args.port is None:
        args.port = 9000 if args.target == "service" else 9001

    if args.settings is None:
        args.settings = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "pyro", "settings.json"
        )

    if args.middleware == "xmlrpc":
        run_xmlrpc(args.target, args.nodes, args.port)
    else:
        uri_list = [u.strip() for u in args.uris.split(",")] if args.uris else []
        run_pyro(args.target, args.nodes, uri_list, args.settings)


if __name__ == "__main__":
    main()
