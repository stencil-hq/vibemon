# Test-only trust hook: the e2b SDK's pyqwest transports verify with the
# platform verifier on macOS (ignores SSL_CERT_FILE); hand them the test CA.
import os
_ca = os.environ.get("VMON_TEST_CA")
if _ca:
    import pyqwest
    _pem = open(_ca, "rb").read()
    def _wrap(cls):
        def make(*args, **kwargs):
            kwargs.setdefault("tls_ca_cert", _pem)
            return cls(*args, **kwargs)
        return make
    pyqwest.HTTPTransport = _wrap(pyqwest.HTTPTransport)
    pyqwest.SyncHTTPTransport = _wrap(pyqwest.SyncHTTPTransport)
