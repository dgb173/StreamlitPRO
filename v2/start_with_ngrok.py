import os
from pyngrok import ngrok, conf

from app import app

NGROK_BIN_PATH = os.getenv('NGROK_BIN_PATH', r"C:\Users\Usuario\Desktop\v2\ngrok.exe")
if NGROK_BIN_PATH:
    conf.get_default().ngrok_path = NGROK_BIN_PATH

if __name__ == '__main__':
    authtoken = os.getenv('NGROK_AUTHTOKEN')
    if authtoken:
        ngrok.set_auth_token(authtoken)

    port = int(os.getenv('APP_PORT', '5000'))
    public_url = ngrok.connect(port)
    print(f'\nURL pública de ngrok: {public_url}\n')
    app.run(host='0.0.0.0', port=port, debug=False)