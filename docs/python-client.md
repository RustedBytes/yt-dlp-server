# Python Client

A small `urllib3` client is available at `examples/python_client.py`.

Install the Python dependency:

```bash
python3 -m pip install urllib3
```

Queue downloads:

```bash
python3 examples/python_client.py submit \
  https://www.tiktok.com/@user/video/123 \
  https://www.instagram.com/reel/ABC/
```

Queue from a text file with one URL per line:

```bash
python3 examples/python_client.py submit --file urls.txt
```

Check a job:

```bash
python3 examples/python_client.py job <job-id>
```
