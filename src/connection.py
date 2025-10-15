import requests

class APIClient:
    def __init__(self, base, email=None, password=None, gln=None, timeout=120):
        self.base = base.rstrip("/")
        self.email = email
        self.password = password
        self.gln = gln
        self.timeout = timeout
        self.token = None
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def set_credentials(self, email, password, gln):
        self.email = email
        self.password = password
        self.gln = gln

    def authenticate(self, verbose=False):
        """Hakee tokenin ja asettaa Authorization-headerin"""
        if not (self.email and self.password and self.gln):
            raise ValueError("Puuttuvat tunnukset: aseta email/password/gln ennen authenticate().")

        r = self.session.post(
            f"{self.base}/Account/Token",
            json={"UserEmail": self.email, "Password": self.password, "Gln": self.gln},
            timeout=30,
        )
        r.raise_for_status()
        try:
            j = r.json()
            token = j.get("token") if isinstance(j, dict) else j
        except Exception:
            token = r.text
        self.token = token.strip()
        self.session.headers["Authorization"] = f"Bearer {self.token}"

        if verbose:
            print("✅ Autentikointi onnistui")

        return self.token

    def call(self, endpoint, payload=None, method="POST"):
        """Yleiskäyttöinen kutsu mille tahansa endpointille."""
        if not self.token:
            raise RuntimeError("Ei tokenia. Kutsu authenticate() ensin.")
        url = endpoint if endpoint.startswith("http") else f"{self.base}{endpoint}"
        method = method.upper()
        if method == "GET":
            r = self.session.get(url, params=payload, timeout=self.timeout)
        else:
            r = self.session.post(url, json=payload, timeout=self.timeout)
        r.raise_for_status()
        try:
            return r.json()
        except ValueError:
            return r.text 


def auth_and_call(base, email, password, gln, endpoint, payload=None, method="POST", verbose_auth=False):
    c = APIClient(base, email, password, gln)
    c.authenticate(verbose=verbose_auth)
    return c.call(endpoint, payload=payload, method=method)
