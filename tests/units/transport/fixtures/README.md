These public, disposable P-256 credentials exist only for transport tests.
`ca.pem` signs separate serverAuth (`localhost` DNS SAN) and clientAuth identities.
Private keys are intentionally checked in; never use them outside tests.
The CA private key is discarded. Certificates expire in October 2036.
