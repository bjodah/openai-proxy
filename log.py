import sys
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
            'request_url': str(self.request_url) if self.request_url else None,
            'request_method': str(self.request_method) if self.request_method else None,
            'request_time': int(self.request_time) if self.request_time else None,
            'response_time': int(self.response_time) if self.response_time else None,
            'status_code': int(self.status_code) if self.status_code else None,
            'request_content': str(self.request_content) if self.request_content else None,
            'response_header': str(self.response_header) if self.response_header else None,
            'response_content': str(self.response_content) if self.response_content else None,
        }


engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Test database connection
try:
    from sqlalchemy import text
    with SessionLocal() as session:
        session.execute(text("SELECT 1"))
    sys.stderr.write("✅ Database connection successful\n")
except Exception as e:
    sys.stderr.write(f"❌ Database connection failed: {e}\n")


async def save_log(log: OpenAILog):
    # Use SQLAlchemy session for compatibility with LogQuery
    session = SessionLocal()
    try:
        session.add(log)
        session.commit()
        sys.stderr.write(f"✅ Saved log entry: ID {log.id} - URL {log.request_url} - Status {log.status_code}\n")
    except Exception as e:
        session.rollback()
        sys.stderr.write(f"❌ Error in save_log (DB operation failed for URL {log.request_url}, Status {log.status_code}): {e}\n")
        raise # Re-raise the exception to be caught by the caller (update_log)
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
                OpenAILog.request_time.between(start, end)
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
        sys.stderr.write("No logs found\n")
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
            print(f"{log_data['id'] or '':<5} | "
                  f"{log_data['request_time'] or '':<20} | "
                  f"{log_data['request_method'] or '':<7} | "
                  f"{log_data['status_code'] or '':<6} | "
                  f"{log_data['request_url'] or '':<60} | "
                  f"{(log_data['request_content'] or log_data['response_content'] or '')[:50]:<50}")

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
    # from typing import List # Already imported at top level
    # from datetime import datetime # Not used directly in main()
    from sqlalchemy import desc, between
    import json # Already imported at top level

    main()
