from fastapi import FastAPI, Request, HTTPException, Depends, status
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import os
import uvicorn
from dotenv import load_dotenv
import httpx 
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from jose import JWTError, jwt
from datetime import datetime, timedelta
import json

# Database and schemas
from database import engine, get_db
import models
import schemas

# Analysis Logic
from analysis_logic import run_analysis

# Load environment variables
load_dotenv()

# --- Configurations ---
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
SECRET_KEY = os.getenv("SECRET_KEY", "a_very_secret_key_that_should_be_in_env_file")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
if not GOOGLE_API_KEY:
    raise ValueError("Missing GOOGLE_API_KEY or GEMINI_API_KEY environment variable.")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not GOOGLE_CREDENTIALS_JSON:
    raise ValueError("Missing GOOGLE_CREDENTIALS_JSON environment variable.")

try:
    GOOGLE_CLIENT_CONFIG = json.loads(GOOGLE_CREDENTIALS_JSON)
except json.JSONDecodeError:
    raise ValueError("GOOGLE_CREDENTIALS_JSON is not a valid JSON string.")
GEMINI_API_KEY = GOOGLE_API_KEY

# --- App Setup ---
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL, "http://localhost", "http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Utility Functions ---
def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

from sqlalchemy.orm import selectinload

# ... (other imports)

@app.get("/api/auth/naver/login")
async def naver_login():
    state = "random_string"  # Replace with a secure random string
    redirect_uri = f"{BACKEND_URL}/api/auth/naver/callback"
    naver_auth_url = f"https://nid.naver.com/oauth2.0/authorize?response_type=code&client_id={NAVER_CLIENT_ID}&redirect_uri={redirect_uri}&state={state}"
    return RedirectResponse(naver_auth_url)

@app.get("/api/auth/naver/callback")
async def naver_callback(request: Request, db: AsyncSession = Depends(get_db)):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    
    redirect_uri = f"{BACKEND_URL}/api/auth/naver/callback"
    token_url = f"https://nid.naver.com/oauth2.0/token?grant_type=authorization_code&client_id={NAVER_CLIENT_ID}&client_secret={NAVER_CLIENT_SECRET}&code={code}&state={state}"

    async with httpx.AsyncClient() as client:
        token_response = await client.get(token_url)
        token_data = token_response.json()
        
        if "access_token" not in token_data:
            raise HTTPException(status_code=400, detail="Could not get access token from Naver")

        access_token = token_data["access_token"]
        
        profile_url = "https://openapi.naver.com/v1/nid/me"
        headers = {"Authorization": f"Bearer {access_token}"}
        profile_response = await client.get(profile_url, headers=headers)
        profile_data = profile_response.json()

        if profile_data["resultcode"] != "00":
            raise HTTPException(status_code=400, detail="Could not get user profile from Naver")

        naver_user_info = profile_data["response"]
        naver_id = naver_user_info["id"]
        email = naver_user_info.get("email")

        # Find user by email first
        user_query = select(models.User).where(models.User.email == email)
        user = (await db.execute(user_query)).scalar_one_or_none()

        if user:
            # User with this email exists, link naver_id if it's not already there
            if not user.naver_id:
                user.naver_id = naver_id
                await db.commit()
        else:
            # No user with this email, check by naver_id
            user_query = select(models.User).where(models.User.naver_id == naver_id)
            user = (await db.execute(user_query)).scalar_one_or_none()
            if not user:
                # If still no user, create a new one
                new_user = models.User(
                    naver_id=naver_id,
                    email=email,
                    name=naver_user_info.get("name")
                )
                db.add(new_user)
                await db.commit()
                await db.refresh(new_user)
                user = new_user

    # Create JWT token with email as subject
    jwt_token = create_access_token(data={"sub": user.email})
    
    # Redirect to frontend with token
    redirect_url = f"{FRONTEND_URL}/naver-callback?token={jwt_token}"
    return RedirectResponse(redirect_url)


from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import auth


# ... (other imports)
async def authenticate_user(email: str, password: str, db: AsyncSession):
    """
    Authenticate user by email and password.
    """
    query = select(models.User).where(models.User.email == email)
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if not user:
        return False
    if not auth.verify_password(password, user.hashed_password):
        return False
    return user

@app.post("/api/token", response_model=schemas.Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    user = await authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": user.email}
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/api/users/register", response_model=schemas.User)
async def register_user(user: schemas.UserCreate, db: AsyncSession = Depends(get_db)):
    # Check if user already exists
    query = select(models.User).where(models.User.email == user.email)
    db_user = (await db.execute(query)).scalar_one_or_none()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    hashed_password = auth.get_password_hash(user.password)
    new_user = models.User(
        email=user.email,
        name=user.name,
        hashed_password=hashed_password
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)
    
    # Manually set has_google_credentials for the response model
    user_response = schemas.User.from_orm(new_user)
    user_response.has_google_credentials = False
    return user_response


async def get_current_user(request: Request, db: AsyncSession = Depends(get_db)):
    auth_header = request.headers.get("Authorization")
    token_param = request.query_params.get("token")
    
    token = None
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split("Bearer ")[1]
    elif token_param:
        token = token_param
    else:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated: No token provided")

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = schemas.TokenData(email=email)
    except JWTError:
        raise credentials_exception

    query = select(models.User).options(selectinload(models.User.google_credentials)).where(models.User.email == token_data.email)
    result = await db.execute(query)
    user = result.scalar_one_or_none()

    if user is None:
        raise credentials_exception
    return user

# ... (other endpoints)

@app.get("/api/me", response_model=schemas.User)
async def read_users_me(current_user: models.User = Depends(get_current_user)):
    user_response = schemas.User.from_orm(current_user)
    user_response.has_google_credentials = current_user.google_credentials is not None
    return user_response

@app.get("/api/auth/google/login")
async def google_login(current_user: models.User = Depends(get_current_user)):
    flow = Flow.from_client_config(GOOGLE_CLIENT_CONFIG, scopes=['https://www.googleapis.com/auth/gmail.readonly'])
    flow.redirect_uri = f"{BACKEND_URL}/api/auth/google/callback"
    
    # Pass user's ID in state to link accounts on callback
    authorization_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true', state=str(current_user.id))
    return RedirectResponse(authorization_url)

@app.get("/api/auth/google/callback")
async def google_auth_callback(request: Request, db: AsyncSession = Depends(get_db)):
    user_id = request.query_params.get('state')
    
    user_query = select(models.User).where(models.User.id == int(user_id))
    user = (await db.execute(user_query)).scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User from state not found.")

    flow = Flow.from_client_config(GOOGLE_CLIENT_CONFIG, scopes=['https://www.googleapis.com/auth/gmail.readonly'])
    flow.redirect_uri = f"{BACKEND_URL}/api/auth/google/callback"
    
    flow.fetch_token(authorization_response=str(request.url))
    creds = flow.credentials

    creds_query = select(models.GoogleCredentials).where(models.GoogleCredentials.user_id == user.id)
    db_creds = (await db.execute(creds_query)).scalar_one_or_none()

    creds_data = {
        "user_id": user.id,
        "token": creds.token, "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri, "client_id": creds.client_id,
        "client_secret": creds.client_secret, "scopes": creds.scopes,
    }

    if db_creds:
        for key, value in creds_data.items():
            setattr(db_creds, key, value)
    else:
        db_creds = models.GoogleCredentials(**creds_data)
        db.add(db_creds)
    
    await db.commit()
    
    return RedirectResponse(f"{FRONTEND_URL}?google_linked=true")

@app.post("/api/analyze/gmail")
async def analyze_user_gmail(current_user: models.User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    if not current_user.google_credentials:
        raise HTTPException(status_code=400, detail="Google account not linked.")

    creds_data = current_user.google_credentials
    credentials = Credentials(
        token=creds_data.token, refresh_token=creds_data.refresh_token,
        token_uri=creds_data.token_uri, client_id=creds_data.client_id,
        client_secret=creds_data.client_secret, scopes=creds_data.scopes
    )

    analysis_results = await run_analysis(credentials, GEMINI_API_KEY, db, current_user.id)
    return analysis_results

@app.get("/api/users/{user_id}/analysis")
async def get_user_analysis(user_id: int, db: AsyncSession = Depends(get_db)):
    query = select(models.GmailAnalysis).where(models.GmailAnalysis.user_id == user_id)
    result = await db.execute(query)
    analysis_results = result.scalars().all()
    if not analysis_results:
        return []
    # Parse the JSON string in analysis_result before returning
    parsed_results = []
    for res in analysis_results:
        try:
            parsed_json = json.loads(res.analysis_result)
            # Add the created_at timestamp to the parsed JSON
            parsed_json["analysis_created_at"] = res.created_at.isoformat()
            parsed_results.append(parsed_json)
        except (json.JSONDecodeError, TypeError):
             # If it's not a valid JSON string, just append the raw value
            parsed_results.append(res.analysis_result)

    return parsed_results
