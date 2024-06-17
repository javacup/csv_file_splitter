import os
import jwt
from jwt import PyJWKClient
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# define token url client id and secret
token_url = os.getenv('TOKEN_URL')

client_id = os.getenv(
    'CLIENT_ID')
client_secret = os.getenv(
    'CLIENT_SECRET')
jwks_url = os.getenv(
    'JWKS_URL')
audience = os.getenv(
    'AUDIENCE')
issuer = os.getenv(
    'ISSUER')

# Define the payload for the token request
payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'email profile openid'
}

# verify if access token is valid


def verify_token(access_token):
    try:
        print(f"Verifying token: {jwks_url}")
        # Fetch the JWKS
        jwks_client = PyJWKClient(jwks_url)
        signing_key = jwks_client.get_signing_key_from_jwt(access_token)

        # Decode and verify the JWT
        decoded_token = jwt.decode(
            access_token,
            signing_key.key,
            algorithms=["RS256"],
            audience=audience,
            issuer=issuer
        )
        # Return the decoded token if it is valid
        return True
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        return False
    except jwt.InvalidTokenError:
        print("Invalid token")
        return False


# write a function to fetch the access token
def get_access_token():

    # check if there is an access token file
    try:
        with open('access_token.txt', 'r') as f:
            access_token = f.read()
            # print(f'Access Token: {access_token}')
            # check if access token is valid
            # if valid, return it
            # if not valid, fetch a new one
            if verify_token(access_token):
                print("SavedToken is valid")
                return access_token
    except FileNotFoundError:
        print('Access Token file not found')

    print("SavedToken is not valid.. fetching new token")

    # Make the POST request to fetch the access token
    response = requests.post(token_url, data=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response to get the access token
        token_response = response.json()
        access_token = token_response['access_token']
        # write access_token to a file
        with open('access_token.txt', 'w') as f:
            f.write(access_token)
    else:
        print(f'Failed to fetch access token. Status code: {
              response.status_code}')
        print(f'Response: {response.text}')
    return access_token


if __name__ == "__main__":
    access_token = get_access_token()
    print(f'Access Token: {access_token}')
