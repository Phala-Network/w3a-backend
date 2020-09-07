const crypto = require('crypto');
const aes = require('node-aes-gcm');

function generateIv() {
  return Buffer.from(crypto.randomBytes(12), 'utf8');
}

function encrypt(secret, iv, data) {
  return aes.encrypt(secret, iv, data, Buffer.from(''));
}

function decrypt(secret, iv, cipher_data, auth_tag) {
  return aes.decrypt(secret, iv, cipher_data, Buffer.from(''), auth_tag);
}

module.exports = { generateIv, encrypt, decrypt }
