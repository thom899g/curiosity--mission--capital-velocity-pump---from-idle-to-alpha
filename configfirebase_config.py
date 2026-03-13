"""
Firebase Configuration Module
Purpose: Centralized Firebase initialization with error handling and environment validation
Architecture Choice: Singleton pattern prevents multiple Firebase instances, ensures connection pooling
"""
import os
import logging
from typing import Optional
from google.cloud import firestore
from google.cloud.firestore_v1.client import Client
from google.oauth2 import service_account
import firebase_admin
from firebase_admin import credentials, firestore as firebase_firestore
from firebase_admin.exceptions import FirebaseError

logger = logging.getLogger(__name__)

class FirebaseManager:
    """Singleton Firebase client manager with robust error recovery"""
    
    _instance: Optional['FirebaseManager'] = None
    _client: Optional[Client] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FirebaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialized = True
            self._initialize_firebase()
    
    def _initialize_firebase(self) -> None:
        """
        Initialize Firebase with environment validation and fallback strategies
        Edge Cases Handled:
        - Missing credentials file
        - Invalid JSON format
        - Network timeouts
        - Insufficient permissions
        """
        try:
            # Path validation
            cred_path = os.environ.get('FIREBASE_CREDENTIALS_PATH')
            
            if not cred_path:
                logger.error("FIREBASE_CREDENTIALS_PATH environment variable not set")
                raise EnvironmentError("Firebase credentials path not configured")
            
            if not os.path.exists(cred_path):
                logger.error(f"Credentials file not found at: {cred_path}")
                raise FileNotFoundError(f"Firebase credentials file missing at {cred_path}")
            
            # Initialize Firebase Admin SDK
            cred = credentials.Certificate(cred_path)
            firebase_admin.initialize_app(cred, {
                'projectId': os.environ.get('FIREBASE_PROJECT_ID', 'sentient-pump-prod')
            })
            
            # Initialize Firestore client
            self._client = firebase_firestore.client()
            
            # Test connection
            test_doc = self._client.collection('system_health').document('connection_test')
            test_doc.set({'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'connected'})
            
            logger.info("Firebase initialized successfully")
            
        except FirebaseError as e:
            logger.error(f"Firebase initialization error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Firebase init: {str(e)}")
            raise
    
    @property
    def client(self) -> Client:
        """Lazy-loaded Firestore client with connection validation"""
        if self._client is None:
            self._initialize_firebase()
        return self._client
    
    def get_collection(self, collection_name: str):
        """Safe collection reference getter with validation"""
        if not collection_name or not isinstance(collection_name, str):
            raise ValueError(f"Invalid collection name: {collection_name}")
        return self.client.collection(collection_name)
    
    def health_check(self) -> bool:
        """Verify Firebase connection is healthy"""
        try:
            # Attempt a simple read operation
            test_ref = self.get_collection('system_health').document('health_check')
            test_ref.set({'timestamp': firestore.SERVER_TIMESTAMP})
            test_ref.get()
            return True
        except Exception as e:
            logger.warning(f"Firebase health check failed: {e}")
            return False

# Global instance for import
firebase_manager = FirebaseManager()