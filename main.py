from aws_utils import AWSUtils
import requests
import time

if __name__ == '__main__':
    utils = AWSUtils()
    # create 2 endpoints
    first_instance, first_public_ip = utils.create_endpoint_instance(2,
                                                                     define_iam=True)
    second_instance, second_public_ip = utils.create_endpoint_instance(2,
                                                                       sibling_ip=first_public_ip,
                                                                       define_iam=True)
    # we need to give the endpoints time to bring up the program
    time.sleep(120)
    # connect the endpoints
    try:
        req = requests.post(f"http://{first_public_ip}:5000/add_sibling?sibling_ip={second_public_ip}")
        if req.status_code == 200:
            j = req.json()
            if j['res'] == 'success':
                print(f'sibling was successfully added')
            else:
                print("couldn't set sibling")
    except requests.exceptions.ConnectionError:
        print("connection error: couldn't set sibling")


