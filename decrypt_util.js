const aead = require('./aead');

const key = "290c3c5d812a4ba7ce33adf09598a462692a615beb6c80fdafb3f9e3bbef8bc6";

function decrypt(cipher) {
  let iv_b64 = cipher.split('|')[0];
  let cipher_b64 = cipher.split('|')[1];

  let iv = Buffer.from(iv_b64, 'base64');

  let cipher_hex = Buffer.from(cipher_b64, 'base64').toString('hex');

  let auth_tag = cipher_hex.slice(-32);
  let cipher_data = cipher_hex.slice(0, -32);

  let plain = aead.decrypt(Buffer.from(key, 'hex'), iv, Buffer.from(cipher_data, 'hex'), Buffer.from(auth_tag, 'hex'));
  if (plain.auth_ok)
    return plain.plaintext.toString('utf8');

  return cipher;
}

module.exports = { decrypt }
