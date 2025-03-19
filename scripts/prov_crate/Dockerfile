FROM python:3.12

COPY ./ /prov_crate
WORKDIR /prov_crate

RUN pip install --no-build-isolation --no-cache-dir -r requirements.txt && \
    chmod +x gen_crate.py

ENTRYPOINT ["/prov_crate/gen_crate.py"]
