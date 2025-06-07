use crate::common::error::{Result, SamsaError};
use crate::proto::AccessTokenInfo;
use base64::{Engine as _, engine::general_purpose};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// JWT Claims structure for Samsa access tokens
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SamsaClaims {
    /// Subject (token ID)
    pub sub: String,
    /// Issued at (timestamp in seconds)
    pub iat: u64,
    /// Expiration time (timestamp in seconds)
    pub exp: u64,
    /// Issuer
    pub iss: String,
    /// Audience
    pub aud: String,
    /// JWT ID (unique identifier for this token)
    pub jti: String,
    /// Custom claims for Samsa
    pub samsa: SamsaTokenClaims,
}

/// Custom Samsa-specific claims
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SamsaTokenClaims {
    /// Token info from the original AccessTokenInfo
    pub token_info: AccessTokenInfo,
    /// Node ID that issued this token
    pub node_id: String,
}

/// JWT service for creating and validating access tokens
#[derive(Clone)]
pub struct JwtService {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    issuer: String,
    audience: String,
    node_id: String,
}

impl std::fmt::Debug for JwtService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtService")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("node_id", &self.node_id)
            .field("encoding_key", &"<redacted>")
            .field("decoding_key", &"<redacted>")
            .finish()
    }
}

impl JwtService {
    /// Create a new JWT service with a secret key
    pub fn new(secret: &str, issuer: String, audience: String, node_id: String) -> Self {
        let encoding_key = EncodingKey::from_secret(secret.as_ref());
        let decoding_key = DecodingKey::from_secret(secret.as_ref());

        Self {
            encoding_key,
            decoding_key,
            issuer,
            audience,
            node_id,
        }
    }

    /// Create a new JWT service from environment variables
    pub fn from_env() -> Result<Self> {
        let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| {
            // Generate a random secret if not provided (for development)
            let random_bytes: [u8; 32] = rand::random();
            general_purpose::STANDARD.encode(random_bytes)
        });

        let issuer = std::env::var("JWT_ISSUER").unwrap_or_else(|_| "samsa".to_string());
        let audience = std::env::var("JWT_AUDIENCE").unwrap_or_else(|_| "samsa-api".to_string());
        let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());

        Ok(Self::new(&secret, issuer, audience, node_id))
    }

    /// Create a JWT token from AccessTokenInfo
    pub fn create_token(&self, info: AccessTokenInfo) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| SamsaError::Internal(format!("Failed to get current time: {}", e)))?
            .as_secs();

        // Use the expires_at from the info, or default to 30 days
        let exp = info.expires_at.unwrap_or(now + 30 * 24 * 60 * 60); // 30 days

        // Generate a unique token ID if not provided
        let token_id = if info.id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            info.id.clone()
        };

        let claims = SamsaClaims {
            sub: token_id.clone(),
            iat: now,
            exp,
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            jti: Uuid::new_v4().to_string(), // Unique JWT ID
            samsa: SamsaTokenClaims {
                token_info: info,
                node_id: self.node_id.clone(),
            },
        };

        let header = Header::new(Algorithm::HS256);
        encode(&header, &claims, &self.encoding_key)
            .map_err(|e| SamsaError::Internal(format!("Failed to encode JWT token: {}", e)))
    }

    /// Validate and decode a JWT token
    pub fn validate_token(&self, token: &str) -> Result<SamsaClaims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);

        let token_data =
            decode::<SamsaClaims>(token, &self.decoding_key, &validation).map_err(|e| {
                match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                        SamsaError::Unauthorized("Token has expired".to_string())
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidToken => {
                        SamsaError::Unauthorized("Invalid token format".to_string())
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                        SamsaError::Unauthorized("Invalid token signature".to_string())
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidIssuer => {
                        SamsaError::Unauthorized("Invalid token issuer".to_string())
                    }
                    jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                        SamsaError::Unauthorized("Invalid token audience".to_string())
                    }
                    _ => SamsaError::Unauthorized(format!("Token validation failed: {}", e)),
                }
            })?;

        Ok(token_data.claims)
    }

    /// Extract token ID from a JWT token without full validation (for lookups)
    /// This is useful for operations like revocation where we need the token ID
    /// but the token might already be expired
    pub fn extract_token_id(&self, token: &str) -> Result<String> {
        // Decode without validation to extract the token ID
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false; // Don't validate expiration
        validation.validate_aud = false; // Don't validate audience
        validation.insecure_disable_signature_validation(); // Don't validate signature

        let token_data = decode::<SamsaClaims>(token, &self.decoding_key, &validation)
            .map_err(|e| SamsaError::Internal(format!("Failed to decode token: {}", e)))?;

        Ok(token_data.claims.sub)
    }

    /// Check if a token is expired
    pub fn is_token_expired(&self, token: &str) -> bool {
        match self.extract_claims_unsafe(token) {
            Ok(claims) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                claims.exp < now
            }
            Err(_) => true, // If we can't decode, consider it expired
        }
    }

    /// Extract claims without any validation (unsafe - for internal use only)
    fn extract_claims_unsafe(&self, token: &str) -> Result<SamsaClaims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        validation.validate_aud = false;
        validation.insecure_disable_signature_validation();

        let token_data = decode::<SamsaClaims>(token, &self.decoding_key, &validation)
            .map_err(|e| SamsaError::Internal(format!("Failed to decode token: {}", e)))?;

        Ok(token_data.claims)
    }

    /// Get the issuer of this JWT service
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Get the audience of this JWT service
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Get the node ID of this JWT service
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

// Add rand dependency for random secret generation
use rand;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AccessTokenScope, ResourceSet, resource_set};

    #[test]
    fn test_jwt_service_creation() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        assert_eq!(service.issuer(), "test-issuer");
        assert_eq!(service.audience(), "test-audience");
        assert_eq!(service.node_id(), "test-node");
    }

    #[test]
    fn test_create_and_validate_token() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        let token_info = AccessTokenInfo {
            id: "test-token-123".to_string(),
            expires_at: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ), // 1 hour from now
            auto_prefix_streams: false,
            scope: Some(AccessTokenScope {
                buckets: Some(ResourceSet {
                    matching: Some(resource_set::Matching::Prefix("test-".to_string())),
                }),
                streams: None,
                access_tokens: None,
                ops: vec![],
            }),
        };

        // Create token
        let token = service.create_token(token_info.clone()).unwrap();
        assert!(!token.is_empty());

        // Validate token
        let claims = service.validate_token(&token).unwrap();
        assert_eq!(claims.sub, "test-token-123");
        assert_eq!(claims.iss, "test-issuer");
        assert_eq!(claims.aud, "test-audience");
        assert_eq!(claims.samsa.token_info.id, "test-token-123");
        assert_eq!(claims.samsa.node_id, "test-node");
    }

    #[test]
    fn test_extract_token_id() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        let token_info = AccessTokenInfo {
            id: "extract-test-456".to_string(),
            expires_at: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            auto_prefix_streams: false,
            scope: None,
        };

        let token = service.create_token(token_info).unwrap();
        let extracted_id = service.extract_token_id(&token).unwrap();
        assert_eq!(extracted_id, "extract-test-456");
    }

    #[test]
    fn test_expired_token_validation() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        let token_info = AccessTokenInfo {
            id: "expired-token".to_string(),
            expires_at: Some(1), // Expired timestamp
            auto_prefix_streams: false,
            scope: None,
        };

        let token = service.create_token(token_info).unwrap();

        // Validation should fail for expired token
        let result = service.validate_token(&token);
        assert!(result.is_err());

        // But we should still be able to extract the token ID
        let token_id = service.extract_token_id(&token).unwrap();
        assert_eq!(token_id, "expired-token");

        // Check if token is expired
        assert!(service.is_token_expired(&token));
    }

    #[test]
    fn test_invalid_token_validation() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        // Test with completely invalid token
        let result = service.validate_token("invalid.token.here");
        assert!(result.is_err());

        // Test with token signed with different secret
        let other_service = JwtService::new(
            "different-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        let token_info = AccessTokenInfo {
            id: "test-token".to_string(),
            expires_at: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            auto_prefix_streams: false,
            scope: None,
        };

        let token = other_service.create_token(token_info).unwrap();
        let result = service.validate_token(&token);
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_generated_token_id() {
        let service = JwtService::new(
            "test-secret",
            "test-issuer".to_string(),
            "test-audience".to_string(),
            "test-node".to_string(),
        );

        let token_info = AccessTokenInfo {
            id: "".to_string(), // Empty ID should be auto-generated
            expires_at: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            auto_prefix_streams: false,
            scope: None,
        };

        let token = service.create_token(token_info).unwrap();
        let claims = service.validate_token(&token).unwrap();

        // Should have generated a UUID
        assert!(!claims.sub.is_empty());
        assert_ne!(claims.sub, "");
        assert!(Uuid::parse_str(&claims.sub).is_ok());
    }
}
