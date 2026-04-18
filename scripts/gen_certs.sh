#!/bin/bash
# scripts/gen_certs.sh
# Generate self-signed certificates for the broker

mkdir -p certs
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes -subj "/C=US/ST=CA/L=SF/O=Gokafka/CN=localhost"

echo "Certificates generated in certs/"
