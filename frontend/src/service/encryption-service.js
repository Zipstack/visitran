/**
 * Encryption service for managing network encryption
 * Provides centralized encryption functionality for API calls
 */

// Sensitive fields that should be encrypted
const SENSITIVE_FIELDS = [
  "password",
  "api_key",
  "access_key",
  "secret_key",
  "token",
  "connection_string",
  "passw",
  "connection_url",
  "key",
  "secret",
  "auth_token",
  "api_token",
  "private_key",
  "client_secret",
  "refresh_token",
  "bearer_token",
  "session_token",
  "encryption_key",
  "master_key",
  "app_secret",
  "webhook_secret",
  "signing_key",
  "encryption_secret",
  "auth_code",
  "verification_token",
  // Database specific fields
  "db_password",
  "database_password",
  "db_secret",
  "database_secret",
  "db_key",
  "database_key",
  "db_token",
  "database_token",
  "db_auth",
  "database_auth",
  "db_credential",
  "database_credential",
  // BigQuery specific fields
  "client_email",
  "client_id",
  "private_key_id",
  "private_key",
  "project_id",
];

class EncryptionService {
  constructor() {
    this.publicKey = null;
    this.isInitialized = false;
    this.initializationPromise = null;
  }

  /**
   * Initialize the encryption service by loading the public key
   * @param {string} orgId - Organization ID to use for API calls
   * @return {Promise<boolean>} True if initialization successful
   */
  async initialize(orgId) {
    if (this.isInitialized) {
      return true;
    }

    if (this.initializationPromise) {
      return this.initializationPromise;
    }

    this.initializationPromise = this._doInitialize(orgId);
    return this.initializationPromise;
  }

  /**
   * Perform the actual initialization
   * @param {string} orgId - Organization ID to use for API calls
   * @return {Promise<boolean>} True if initialization successful
   */
  async _doInitialize(orgId) {
    try {
      // Load public key from backend
      await this.loadPublicKey(orgId);

      this.isInitialized = true;
      return true;
    } catch (error) {
      console.error("❌ Failed to initialize encryption service:", error);
      this.isInitialized = false;
      this.publicKey = null;
      throw error;
    } finally {
      this.initializationPromise = null;
    }
  }

  /**
   * Load RSA public key from backend
   * @param {string} orgId - Organization ID to use for API calls
   * @return {Promise<string>} Public key in PEM format
   */
  async loadPublicKey(orgId) {
    try {
      const response = await fetch(
        `/api/v1/visitran/${orgId}/security/public-key`
      );
      if (!response.ok) {
        throw new Error(`Failed to fetch public key: ${response.status}`);
      }

      const data = await response.json();
      if (data.status === "success" && data.data?.public_key) {
        this.publicKey = data.data.public_key;
        return this.publicKey;
      } else {
        throw new Error("Invalid public key response format");
      }
    } catch (error) {
      console.error("Error loading public key:", error);
      throw error;
    }
  }

  /**
   * Check if encryption service is available
   * @return {boolean} True if encryption service is available
   */
  isAvailable() {
    return this.isInitialized && this.publicKey !== null;
  }

  /**
   * Get the loaded public key
   * @return {string|null} Public key in PEM format or null
   */
  getPublicKey() {
    return this.publicKey;
  }

  /**
   * Get encryption status
   * @return {Object} Encryption status information
   */
  getStatus() {
    return {
      isInitialized: this.isInitialized,
      isAvailable: this.isAvailable(),
      hasPublicKey: this.publicKey !== null,
    };
  }

  /**
   * Encrypt a single value using RSA public key
   * @param {string} value - The value to encrypt
   * @return {Promise<string|null>} Encrypted value in base64 format or null if failed
   */
  async encryptValue(value) {
    if (!this.isAvailable()) {
      console.warn("Encryption service not available");
      return null;
    }

    try {
      // Import the public key
      const importedKey = await crypto.subtle.importKey(
        "spki",
        this._pemToArrayBuffer(this.publicKey),
        {
          name: "RSA-OAEP",
          hash: "SHA-256",
        },
        false,
        ["encrypt"]
      );

      // Convert string to Uint8Array
      const encoder = new TextEncoder();
      const data = encoder.encode(value);

      // Check if data is too large for RSA encryption (max ~190 bytes for 2048-bit key)
      const maxChunkSize = 180; // Conservative limit for RSA-OAEP

      if (data.length <= maxChunkSize) {
        // Small data - encrypt directly
        const encrypted = await crypto.subtle.encrypt(
          {
            name: "RSA-OAEP",
            hash: "SHA-256",
          },
          importedKey,
          data
        );

        // Convert to base64
        const encryptedArray = new Uint8Array(encrypted);
        const base64 = btoa(String.fromCharCode(...encryptedArray));

        return base64;
      } else {
        // Large data - use chunked encryption
        const chunks = [];
        for (let i = 0; i < data.length; i += maxChunkSize) {
          const chunk = data.slice(i, i + maxChunkSize);

          const encryptedChunk = await crypto.subtle.encrypt(
            {
              name: "RSA-OAEP",
              hash: "SHA-256",
            },
            importedKey,
            chunk
          );

          const encryptedArray = new Uint8Array(encryptedChunk);
          const base64Chunk = btoa(String.fromCharCode(...encryptedArray));
          chunks.push(base64Chunk);
        }

        // Combine chunks with a delimiter
        const result = chunks.join("|");
        return result;
      }
    } catch (error) {
      console.error("❌ Error encrypting value:", error);
      return null;
    }
  }

  /**
   * Encrypt BigQuery credentials specifically
   * @param {string} credentialsJson - The BigQuery credentials JSON string
   * @return {Promise<string>} Encrypted credentials JSON string
   */
  async encryptBigQueryCredentials(credentialsJson) {
    if (!this.isAvailable()) {
      console.warn(
        "Encryption service not available, returning original credentials"
      );
      return credentialsJson;
    }

    try {
      // Parse the credentials JSON
      const credentials = JSON.parse(credentialsJson);

      // Encrypt sensitive fields within the credentials
      const encryptedCredentials = { ...credentials };

      // List of sensitive fields in BigQuery service account JSON
      const bigQuerySensitiveFields = [
        "private_key",
        "client_email",
        "client_id",
        "private_key_id",
        "project_id",
      ];

      for (const field of bigQuerySensitiveFields) {
        if (encryptedCredentials[field]) {
          if (typeof encryptedCredentials[field] === "string") {
            const encrypted = await this.encryptValue(
              encryptedCredentials[field]
            );
            if (encrypted) {
              encryptedCredentials[field] = encrypted;
            }
          }
        }
      }

      // Return the encrypted credentials as a JSON string
      const result = JSON.stringify(encryptedCredentials);
      return result;
    } catch (error) {
      console.error("❌ Error encrypting BigQuery credentials:", error);
      return credentialsJson; // Return original on error
    }
  }

  /**
   * Encrypt sensitive fields in an object
   * @param {Object} data - The object containing sensitive fields
   * @return {Promise<Object>} Object with sensitive fields encrypted
   */
  async encryptSensitiveFields(data) {
    if (!this.isAvailable()) {
      console.warn("Encryption service not available, returning original data");
      return data;
    }

    // Double-check that we have a public key before proceeding
    if (!this.publicKey) {
      console.error("No public key available for encryption");
      return data;
    }

    return await this._encryptObjectRecursively(data);
  }

  /**
   * Recursively encrypt sensitive fields in an object
   * @param {Object} data - The object containing sensitive fields
   * @return {Promise<Object>} Object with sensitive fields encrypted
   */
  async _encryptObjectRecursively(data) {
    if (!data || typeof data !== "object") {
      return data;
    }

    const encryptedData = { ...data };

    for (const [key, value] of Object.entries(data)) {
      if (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
      ) {
        // Recursively encrypt nested objects
        encryptedData[key] = await this._encryptObjectRecursively(value);
      } else if (
        SENSITIVE_FIELDS.includes(key.toLowerCase()) &&
        typeof value === "string" &&
        value.trim()
      ) {
        // Encrypt sensitive string fields
        const encrypted = await this.encryptValue(value);
        if (encrypted) {
          encryptedData[key] = encrypted;
        }
      } else if (
        key === "credentials" &&
        typeof value === "string" &&
        value.trim()
      ) {
        // Special handling for BigQuery credentials
        try {
          // Check if it's valid JSON (BigQuery credentials)
          JSON.parse(value);
          const encrypted = await this.encryptBigQueryCredentials(value);
          encryptedData[key] = encrypted;
        } catch (e) {
          // Not valid JSON, treat as regular sensitive field
          const encrypted = await this.encryptValue(value);
          if (encrypted) {
            encryptedData[key] = encrypted;
          }
        }
      }
    }

    return encryptedData;
  }

  /**
   * Convert PEM format to ArrayBuffer for Web Crypto API
   * @param {string} pem - PEM formatted key
   * @return {ArrayBuffer} ArrayBuffer representation
   */
  _pemToArrayBuffer(pem) {
    // Remove PEM headers and convert to base64
    const base64 = pem
      .replace(/-----BEGIN PUBLIC KEY-----/, "")
      .replace(/-----END PUBLIC KEY-----/, "")
      .replace(/\s/g, "");

    // Convert base64 to ArrayBuffer
    const binaryString = atob(base64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes.buffer;
  }

  /**
   * Reset the encryption service (for testing or re-initialization)
   */
  reset() {
    this.publicKey = null;
    this.isInitialized = false;
    this.initializationPromise = null;
  }
}

// Create singleton instance
const encryptionService = new EncryptionService();

export default encryptionService;
