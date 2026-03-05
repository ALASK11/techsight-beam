FROM apache/beam_python3.11_sdk:2.61.0

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Pre-cache the tldextract PSL snapshot so workers never hit the network.
RUN python -c "import tldextract; tldextract.TLDExtract(suffix_list_urls=())('example.com')"

COPY src/ /app/src/
COPY setup.py /app/setup.py
RUN pip install --no-cache-dir /app

WORKDIR /app
