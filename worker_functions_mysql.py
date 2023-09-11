from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from mysql.connector import OperationalError
import datetime, json

# Initialize SQLAlchemy engine and session
engine = create_engine(
    'mysql+mysqlconnector://root@localhost:3307/vehicles',
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,  # 30 seconds
    pool_recycle=1800,  # 30 minutes
)
Session = sessionmaker(bind=engine)
MAX_RETRIES = 3
# Define SQLAlchemy Base and tables
Base = declarative_base()

class VehicleEvents(Base):
    __tablename__ = 'vehicle_events'
    id = Column(Integer, primary_key=True)
    vehicle_id = Column(String(255))
    event_time = Column(DateTime)
    event_source = Column(String(255))
    event_type = Column(String(255))
    event_value = Column(String(255))
    event_extra_data = Column(String(255))

class VehicleStatus(Base):
    __tablename__ = 'vehicle_status'
    id = Column(Integer, primary_key=True)
    vehicle_id = Column(String(255))
    report_time = Column(DateTime)
    status_source = Column(String(255))
    status = Column(String(255))

# Create tables
Base.metadata.create_all(engine)


def perform_commit(new_obj:Base):
    session = Session()
    retries = 0
    while retries < MAX_RETRIES:
        try:
            session.add(new_obj)
            session.commit()
            break
        except OperationalError:
            print("MySQL Connection not available. Retrying...")
            session.rollback()
            retries += 1
        except Exception as e:
            session.rollback()
            print(f"Error: {e}")
            break
    if retries >= MAX_RETRIES:
        print("Max retries reached. Exiting.")
    session.close()
def insert_vehicle_event(event):
    new_event = VehicleEvents(
        vehicle_id=event['vehicle_id'],
        event_time=datetime.datetime.fromisoformat(event['event_time'].replace('Z', '+00:00')),
        event_source=event['event_source'],
        event_type=event['event_type'],
        event_value=event['event_value'],
        event_extra_data=json.dumps(event.get('event_extra_data', {}))
    )
    perform_commit(new_obj=new_event)


def insert_vehicle_status(status):
    new_status = VehicleStatus(
        vehicle_id=status['vehicle_id'],
        report_time=datetime.datetime.fromisoformat(status['report_time'].replace('Z', '+00:00')),
        status_source=status['status_source'],
        status=status['status']
    )
    perform_commit(new_obj=new_status)