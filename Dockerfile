FROM python:3.10

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "flask", "--app", "main", "run", "--host", "0.0.0.0" ]
EXPOSE 5000