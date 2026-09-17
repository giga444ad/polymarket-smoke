import { privateKeyToAccount } from "viem/accounts";
import axios from "axios";

// ================= НАСТРОЙКИ =================
const PRIVATE_KEY = "0xe491f482fbbe296ab7a3ceaa22da97a8c7036c1980cbb040797215ed04b95a90";
const FUNDER_SAFE_ADDRESS = "0xED2D97300352e1A8ad595932253CFbE109706265"; // Ваш Safe из окна депозита

const account = privateKeyToAccount(PRIVATE_KEY);

// 1. Получение или создание API Ключей в CLOB API
async function getOrDeriveApiKeys() {
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const nonce = 0;

  const domain = {
    name: "ClobAuthDomain",
    version: "1",
    chainId: 137, // Polygon Mainnet
  };

  const types = {
    ClobAuth: [
      { name: "address", type: "address" },
      { name: "timestamp", type: "string" },
      { name: "nonce", type: "uint256" },
      { name: "message", type: "string" },
    ],
  };

  const message = {
    address: account.address,
    timestamp: timestamp,
    nonce: BigInt(nonce),
    message: "This message attests that I control the given wallet",
  };

  // Подписываем EIP-712 сообщение
  const signature = await account.signTypedData({
    domain,
    types,
    primaryType: "ClobAuth",
    message,
  });

  const headers = {
    POLY_ADDRESS: account.address,
    POLY_SIGNATURE: signature,
    POLY_TIMESTAMP: timestamp,
    POLY_NONCE: nonce.toString(),
  };

  try {
    const createRes = await axios.post(
      "https://clob.polymarket.com/auth/api-key",
      {},
      { headers }
    );
    return createRes.data;
  } catch (error: any) {
    if (error.response?.status === 400 || error.response?.status === 409) {
      // Ключи уже созданы, восстанавливаем их
      const deriveRes = await axios.get(
        "https://clob.polymarket.com/auth/derive-api-key",
        { headers }
      );
      return deriveRes.data;
    }
    throw new Error(`Ошибка генерации API ключей: ${error.message}`);
  }
}

async function main() {
  console.log(`Инициализация для EOA адреса: ${account.address}\n`);

  console.log("1. Генерация / получение API-ключей...");
  const creds = await getOrDeriveApiKeys();

  console.log("\n🎉 ВСЕ КЛЮЧИ И КОНФИГУРАЦИЯ ПОЛУЧЕНЫ!");
  console.log("-----------------------------------------------------");
  console.log(`SIGNER (EOA):       ${account.address}`);
  console.log(`FUNDER (Safe):      ${FUNDER_SAFE_ADDRESS}`);
  console.log(`SIGNATURE_TYPE:     2`);
  console.log(`API_KEY:            ${creds.apiKey}`);
  console.log(`API_SECRET:         ${creds.secret}`);
  console.log(`API_PASSPHRASE:     ${creds.passphrase}`);
  console.log("-----------------------------------------------------");
}

main().catch(console.error);