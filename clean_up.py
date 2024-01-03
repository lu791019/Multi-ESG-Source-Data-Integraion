import base64
import json
import re
import requests

URL = 'https://harbor.wistron.com/api/v2.0'
PRODUCT_NAME = 'k8sprdwhqecossot2021'
REPOSITORY_NAME = 'eco-ssot-etl'
USERNAME = 'robot$k8sprdwhqecossot2021+gitlab-runner'
TOKEN = 'wW3VmH7dxhYe6l1g7DpSJyiCA4bYWI72'

credentials = f'{USERNAME}:{TOKEN}'
encoded_credentials = base64.b64encode(
    credentials.encode('utf-8')).decode('utf-8')
headers = {'Authorization': f'Basic {encoded_credentials}'}


def removeListResponseByReference(ref):
    try:
        removeResponse = requests.delete(
            f'{URL}/projects/{PRODUCT_NAME}/repositories/{REPOSITORY_NAME}/artifacts/{ref}', headers=headers,
            verify=False)
        print(removeResponse.text)
    except requests.Timeout:
        print("Request Time out")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")


def main():
    listResponse = requests.get(
        f'{URL}/projects/{PRODUCT_NAME}/repositories/{REPOSITORY_NAME}/artifacts?page_size=100', headers=headers,
        verify=False)

    data = json.loads(listResponse.text)
    data = list(filter(lambda x: x['tags'] is not None, data))

    refs = list(
        map(
            lambda x: x['digest'],
            filter(lambda x:  any(
                re.match(r'.*-dev$', tags['name']) for tags in x['tags']), data),
        )
    )[3:]
    print(refs)

    for ref in refs:
        removeListResponseByReference(ref)


if __name__ == "__main__":
    main()
