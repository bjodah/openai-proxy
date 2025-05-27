import databases
from typing import List

from sqlalchemy import create_engine, Column, Integer, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# database config
DATABASE_URL = 'sqlite:///./openai_log.db'
database = databases.Database(DATABASE_URL)
Base = declarative_base()


# database model
class OpenAILog(Base):
    """
    OpenAI API call log
    """
    __tablename__ = 'openai_logs'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    request_url = Column(String, nullable=False)
    request_method = Column(String, nullable=False)
    request_time = Column(BigInteger, nullable=False)
    response_time = Column(BigInteger, nullable=True)
    status_code = Column(Integer, nullable=True)
    request_content = Column(String, nullable=True)
    response_header = Column(String, nullable=True)
    response_content = Column(String, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'request_url': self.request_url,
            'request_method': self.request_method,
            'request_time': self.request_time,
            'response_time': self.response_time,
            'status_code': self.status_code,
            'request_content': self.request_content,
            'response_header': self.response_header,
            'response_content': self.response_content,
        }


engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Test database connection
try:
    from sqlalchemy import text
    with SessionLocal() as session:
        session.execute(text("SELECT 1"))
    print("✅ Database connection successful")
except Exception as e:
    print(f"❌ Database connection failed: {e}")


async def save_log(log: OpenAILog):
    # Use SQLAlchemy session for compatibility with LogQuery
    session = SessionLocal()
    try:
        session.add(log)
        session.commit()
        print(f"✅ Saved log entry: {log.id} - {log.request_url} - {log.status_code}")
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()


class LogQuery:
    """Helper class for querying logs"""
    
    @staticmethod
    def get_all(limit: int = 100) -> List[OpenAILog]:
        """Get all logs with optional limit"""
        session = SessionLocal()
        try:
            return session.query(OpenAILog).order_by(OpenAILog.request_time.desc()).limit(limit).all()
        finally:
            session.close()

    @staticmethod
    def by_status(status_code: int) -> List[OpenAILog]:
        """Filter logs by status code"""
        session = SessionLocal()
        try:
            return session.query(OpenAILog).filter(OpenAILog.status_code == status_code).all()
        finally:
            session.close()

    @staticmethod
    def by_time_range(start: int, end: int) -> List[OpenAILog]:
        """Get logs within a time range"""
        session = SessionLocal()
        try:
            return session.query(OpenAILog).filter(
                between(OpenAILog.request_time, start, end)
            ).all()
        finally:
            session.close()

    @staticmethod
    def count() -> int:
        """Get total log count"""
        session = SessionLocal()
        try:
            return session.query(OpenAILog).count()
        finally:
            session.close()

def print_logs(logs: List[OpenAILog], output_format: str = 'pretty'):
    """Print logs in specified format"""
    if not logs:
        print("No logs found")
        return

    if output_format == 'json':
        print(json.dumps([log.to_dict() for log in logs], indent=2))
    else:
        # Pretty-printed table
        width = 150
        print('-' * width)
        print(f"{'ID':<5} | {'Time':<20} | {'Method':<7} | {'Status':<6} | {'URL':<60} | {'Content Excerpt':<50}")
        print('-' * width)
        for log in logs:
            log_data = log.to_dict()
            print(f"{log_data['id']:<5} | {log_data['request_time']:<20} | {log_data['request_method']:<7} | "
                  f"{log_data['status_code']:<6} | {log_data['request_url']:<60} | "
                  f"{log_data['request_content'] or log_data['response_content']:<50}")

def main():
    """Command line interface for log inspection"""
    parser = argparse.ArgumentParser(
        description='OpenAI Proxy Log Inspector',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""Examples:
  View last 10 logs (default):
    python log.py
  
  View last 50 logs:
    python log.py -r 50
  
  View all 500 errors:
    python log.py -s 500
  
  View logs between timestamps:
    python log.py -ts 1672531200000 -te 1672617600000
  
  Count all logs:
    python log.py -c
  
  JSON output:
    python log.py -r 5 -f json"""
    )
    parser.add_argument(
        '--recent', '-r',
        type=int,
        help='Show N most recent logs (default: 10)'
    )
    parser.add_argument(
        '--status', '-s',
        type=int,
        help='Filter by HTTP status code (e.g. 200, 404, 500)'
    )
    parser.add_argument(
        '--time-start', '-ts',
        type=int,
        help='Start timestamp in milliseconds since epoch'
    )
    parser.add_argument(
        '--time-end', '-te',
        type=int,
        help='End timestamp in milliseconds since epoch'
    )
    parser.add_argument(
        '--count', '-c',
        action='store_true',
        help='Show total log count'
    )
    parser.add_argument(
        '--format', '-f',
        choices=['pretty', 'json'],
        default='pretty',
        help='Output format: pretty (human-readable) or json (machine-readable)'
    )

    args = parser.parse_args()

    if args.count:
        print(f"Total logs: {LogQuery.count()}")
    elif args.status:
        logs = LogQuery.by_status(args.status)
        print_logs(logs, args.format)
    elif args.time_start and args.time_end:
        logs = LogQuery.by_time_range(args.time_start, args.time_end)
        print_logs(logs, args.format)
    else:
        logs = LogQuery.get_all(limit=args.recent or 10)
        print_logs(logs, args.format)

if __name__ == '__main__':
    import argparse
    from typing import List
    from datetime import datetime
    from sqlalchemy import desc, between
    import json
    
    main()
