# sample script to create cert and user
cockroach cert create-ca --certs-dir=certs --ca-key=cert-keys/ca.key

cockroach cert create-node localhost $(hostname) --certs-dir=certs --ca-key=cert-keys/ca.key

cockroach cert create-client root --certs-dir=certs --ca-key=cert-keys/ca.key

cockroach start-single-node --certs-dir=certs --listen-addr=localhost:26257 --http-addr=localhost:8080 --background

cockroach cert create-client maxroach --certs-dir=certs --ca-key=cert-keys/ca.key