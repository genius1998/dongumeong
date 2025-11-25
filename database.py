import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

# .env 파일에서 환경 변수 로드
load_dotenv()

SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")

# 비동기용 엔진 생성 (PostgreSQL 용으로 수정)
# SQLALCHEMY_DATABASE_URL이 None인 경우에 대한 예외 처리 추가
if SQLALCHEMY_DATABASE_URL is None:
    raise ValueError("DATABASE_URL 환경 변수를 설정해야 합니다.")

# Render 배포 시 기본 postgresql:// 주소를 비동기 드라이버용으로 변경
if SQLALCHEMY_DATABASE_URL.startswith("postgresql://"):
    SQLALCHEMY_DATABASE_URL = SQLALCHEMY_DATABASE_URL.replace(
        "postgresql://", "postgresql+asyncpg://", 1
    )

engine = create_async_engine(SQLALCHEMY_DATABASE_URL)

# 비동기용 세션 생성
AsyncSessionLocal = async_sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 모든 모델이 상속할 기본 클래스
Base = declarative_base()

# 의존성 주입을 위한 함수
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db
