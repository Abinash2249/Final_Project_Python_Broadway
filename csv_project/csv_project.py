import csv
import psycopg2

def estd_connection():
    conn = psycopg2.connect(
        database="responses",
        user="postgres",
        password="4321",
        host="127.0.0.1",
        port=5432
    )

    conn.autocommit = True
    print("Connection successfully established!")
    cursor = conn.cursor()
    return cursor

cursor = estd_connection()


create_table_sql = """
    CREATE TABLE responses (
        Respondent INT PRIMARY KEY,
        MainBranch TEXT,
        Hobbyist TEXT,
        OpenSourcer TEXT,
        OpenSource TEXT,
        Employment TEXT,
        Country TEXT,
        Student TEXT,
        EdLevel TEXT,
        UndergradMajor TEXT,
        EduOther TEXT,
        OrgSize TEXT,
        DevType TEXT,
        YearsCode TEXT,
        Age1stCode TEXT,
        YearsCodePro TEXT,
        CareerSat TEXT,
        JobSat TEXT,
        MgrIdiot TEXT,
        MgrMoney TEXT,
        MgrWant TEXT,
        JobSeek TEXT,
        LastHireDate TEXT,
        LastInt TEXT,
        FizzBuzz TEXT,
        JobFactors TEXT,
        ResumeUpdate TEXT,
        CurrencySymbol TEXT,
        CurrencyDesc TEXT,
        CompTotal TEXT,
        CompFreq TEXT,
        ConvertedComp TEXT,
        WorkWeekHrs TEXT,
        WorkPlan TEXT,
        WorkChallenge TEXT,
        WorkRemote TEXT,
        WorkLoc TEXT,
        ImpSyn TEXT,
        CodeRev TEXT,
        CodeRevHrs TEXT,
        UnitTests TEXT,
        PurchaseHow TEXT,
        PurchaseWhat TEXT,
        LanguageWorkedWith TEXT,
        LanguageDesireNextYear TEXT,
        DatabaseWorkedWith TEXT,
        DatabaseDesireNextYear TEXT,
        PlatformWorkedWith TEXT,
        PlatformDesireNextYear TEXT,
        WebFrameWorkedWith TEXT,
        WebFrameDesireNextYear TEXT,
        MiscTechWorkedWith TEXT,
        MiscTechDesireNextYear TEXT,
        DevEnviron TEXT,
        OpSys TEXT,
        Containers TEXT,
        BlockchainOrg TEXT,
        BlockchainIs TEXT,
        BetterLife TEXT,
        ITperson TEXT,
        OffOn TEXT,
        SocialMedia TEXT,
        Extraversion TEXT,
        ScreenName TEXT,
        SOVisit1st TEXT,
        SOVisitFreq TEXT,
        SOVisitTo TEXT,
        SOFindAnswer TEXT,
        SOTimeSaved TEXT,
        SOHowMuchTime TEXT,
        SOAccount TEXT,
        SOPartFreq TEXT,
        SOJobs TEXT,
        EntTeams TEXT,
        SOComm TEXT,
        WelcomeChange TEXT,
        SONewContent TEXT,
        Age TEXT,
        Gender TEXT,
        Trans TEXT,
        Sexuality TEXT,
        Ethnicity TEXT,
        Dependents TEXT,
        SurveyLength TEXT,
        SurveyEase TEXT
    )
"""
cursor.execute(create_table_sql)
print("Table 'responses' created successfully!")

filename = "survey_results_public.csv"

with open(filename, "r", encoding='utf-8') as cr:
    responses = csv.DictReader(cr)
    for index, response in enumerate(responses):
        if index >= 50:
            break

        sql = f"""
            INSERT INTO responses ({', '.join(response.keys())})
            VALUES ({', '.join(['%s']*len(response))})
        """
        cursor.execute(sql, list(response.values()))

    print("First 50 rows successfully inserted!")

cursor.close()
