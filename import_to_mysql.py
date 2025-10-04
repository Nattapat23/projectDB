import pandas as pd
from sqlalchemy import create_engine

# ------------------ ช่วย clean ค่า \N ------------------
def clean_null(x):
    if pd.isna(x) or x == '\\N':
        return None
    return x

def to_int(x):
    try:
        return int(x)
    except:
        return None

def to_boolean(x):
    try:
        return bool(int(x))
    except:
        return None

# ------------------ MySQL Engine ------------------
USER = 'root'
PASSWORD = 'nattapat23'
HOST = '127.0.0.1'
DB = 'Luminix'

engine = create_engine(f'mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DB}')

# ------------------ ฟังก์ชันนำเข้า ------------------
def import_tsv(file_path, table_name, column_mapping, dtypes={}, chunksize=50000):
    print(f"Importing {file_path} -> {table_name} ...")

    # โหลด ID ที่มีอยู่แล้วจากฐาน
    existing_content_ids = set(pd.read_sql("SELECT ContentID FROM Content", engine)['ContentID'])
    existing_person_ids = set(pd.read_sql("SELECT PersonID FROM Person", engine)['PersonID'])

    for chunk in pd.read_csv(file_path, sep='\t', chunksize=chunksize, dtype=str):
        # rename columns
        chunk.rename(columns=column_mapping, inplace=True)

        # clean \N
        chunk = chunk.applymap(clean_null)

        # convert datatypes
        for col, func in dtypes.items():
            if col in chunk.columns:
                chunk[col] = chunk[col].apply(func)

        # กรองเฉพาะแถวที่มี ID อยู่จริง
        if 'PersonID' in chunk.columns:
            chunk = chunk[
            chunk['ContentID'].isin(existing_content_ids) & chunk['PersonID'].isin(existing_person_ids)]
        else:
    # ถ้าไฟล์นี้ไม่มี PersonID (เช่น ratings)
             chunk = chunk[chunk['ContentID'].isin(existing_content_ids)]


        # ส่งเข้า MySQL
        if not chunk.empty:
            chunk.to_sql(table_name, engine, if_exists='append', index=False)

    print(f"Finished importing {table_name}.")

# ------------------ เรียกนำเข้าไฟล์แต่ละตาราง ------------------

# 1. Content
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/title.basics.tsv',
    'Content',
    column_mapping={
        'tconst': 'ContentID',
        'titleType': 'ContentType',
        'primaryTitle': 'Title',
        'originalTitle': 'OriginalTitle',
        'isAdult': 'IsAdult',
        'startYear': 'ReleaseYear',
        'endYear': 'EndYear',
        'runtimeMinutes': 'RuntimeMinutes',
        'genres': 'Genres'
    },
    dtypes={
        'IsAdult': to_boolean,
        'ReleaseYear': to_int,
        'EndYear': to_int,
        'RuntimeMinutes': to_int
    }
)

# 2. ContentCrew
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/title.crew.tsv',
    'ContentCrew',
    column_mapping={
        'tconst': 'ContentID',
        'directors': 'Directors',
        'writers': 'Writers'
    }
)

# 3. Episode
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/title.episode.tsv',
    'Episode',
    column_mapping={
        'tconst': 'EpisodeID',
        'parentTconst': 'SeriesID',
        'seasonNumber': 'SeasonNumber',
        'episodeNumber': 'EpisodeNumber'
    },
    dtypes={
        'SeasonNumber': to_int,
        'EpisodeNumber': to_int
    }
)

# 4. Person
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/name.basics.tsv',
    'Person',
    column_mapping={
        'nconst': 'PersonID',
        'primaryName': 'Name',
        'birthYear': 'BirthYear',
        'deathYear': 'DeathYear',
        'primaryProfession': 'PrimaryProfession',
        'knownForTitles': 'KnownForTitles'
    },
    dtypes={
        'BirthYear': to_int,
        'DeathYear': to_int
    }
)

# 5. ContentPerson
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/title.principals.tsv',
    'ContentPerson',
    column_mapping={
        'tconst': 'ContentID',
        'ordering': 'Ordering',
        'nconst': 'PersonID',
        'category': 'RoleCategory',
        'job': 'Job',
        'characters': 'Characters'
    },
    dtypes={
        'Ordering': to_int
    }
)

# 6. Rating
import_tsv(
    '/Users/nattapatchakkuruang/Desktop/projectDB/title.ratings.tsv',
    'Rating',
    column_mapping={
        'tconst': 'ContentID',
        'averageRating': 'AverageRating',
        'numVotes': 'NumVotes'
    },
    dtypes={
        'AverageRating': lambda x: float(x) if x else None,
        'NumVotes': to_int
    }
)