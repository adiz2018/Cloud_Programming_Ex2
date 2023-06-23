from aws_utils import AWSUtils
import requests

if __name__ == '__main__':
    utils = AWSUtils()
    # create 2 endpoints
    first_instance, first_public_ip = utils.create_endpoint_instance(2,
                                                                     define_iam=True,
                                                                     create_keypair=True)
    second_instane, second_public_ip = utils.create_endpoint_instance(2,
                                                                      sibling_ip=first_public_ip,
                                                                      define_iam=True,
                                                                      create_keypair=True)
    # connect the endpoints
    req = requests.post(f"http://{first_public_ip}:5000/add_sibling?sibling_ip={second_public_ip}")
    if req.status_code == '200':
        j = req.json()
        if j['res'] == 'success':
            print(f'sibling was succesfully added')


