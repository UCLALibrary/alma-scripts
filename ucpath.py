import pymssql
from database_credentials import UCPATH


def get_ucpath_query() -> str:
    return """
with ucla_people as (
    select
        EMPLID
    ,    left(UC_EXT_SYSTEM_ID, 9) as UCLA_UID
    from PS_UC_EXT_SYSTEM es
    where UC_EXT_SYSTEM = 'UCLA_UID'
    and BUSINESS_UNIT in ('LACMP', 'LAMED')
    and DML_IND <> 'D'
    and EFFDT = (
        select max(EFFDT)
        from PS_UC_EXT_SYSTEM
        where EMPLID = es.EMPLID
        and UC_EXT_SYSTEM = 'UCLA_UID'
        and BUSINESS_UNIT in ('LACMP', 'LAMED')
        and DML_IND <> 'D'
        and EFFDT <= {fn curDaTe()}
    )
)
, ucla_employees as (
    -- Connect people with job info.
    -- Currently there's only one duplication on EMPLID,
    -- due to LACMP and LAMED UCLA_UID rows in PS_UC_EXT_SYSTEM.
    -- That leads to duplicate data in the query, so DISTINCT it.
    select distinct
        p.EMPLID
    ,    p.UCLA_UID
    ,    j.EMPL_STATUS
    ,    j.EFFDT
    ,    j.EFFSEQ
    ,    j.EMPL_CLASS
    ,    j.DEPTID
    ,    j.POSITION_NBR
    ,    pjt.DESCR
    ,    pjt.DESCRSHORT
    from ucla_people p
    inner join PS_JOB j on p.EMPLID = j.EMPLID
    inner join PS_JOBCODE_TBL pjt on j.JOBCODE = pjt.JOBCODE
    where j.BUSINESS_UNIT in ('LACMP', 'LAMED')
    -- All active in one way
    and j.EMPL_STATUS in ('A','L','P','W')
    -- Primary job record
    and j.JOB_INDICATOR = 'P'
    -- EMPloyee or CWY (contingent worker)
    and j.PER_ORG = 'EMP'
    -- Single space, avoid some old converted data
    and j.POSITION_NBR <> ' '
    and j.DML_IND <> 'D'
    and j.EFFDT = (
        select max(EFFDT)
        from PS_JOB
        where EMPLID = j.EMPLID
        and EMPL_RCD = j.EMPL_RCD
        and DML_IND <> 'D'
        and EFFDT <= {fn curDaTe()}
    )
    and j.EFFSEQ = (
        select max(EFFSEQ)
        from PS_JOB
        where EMPLID = j.EMPLID
        and EMPL_RCD = j.EMPL_RCD
        and EFFDT = j.EFFDT
        and DML_IND <> 'D'
    )
    and pjt.EFFDT = (
        select max(EFFDT)
        from PS_JOBCODE_TBL
        where JOBCODE = j.JOBCODE
        and DML_IND <> 'D'
    )
)
select
  u.UCLA_UID as employee_id
, u.EMPLID -- for debugging
, n.PREF_FIRST_NAME as emp_first_name
, n.SECOND_LAST_NAME as emp_middle_name
, n.PARTNER_LAST_NAME as emp_last_name
, left(e.EMAIL_ADDR, 50) as email_addr
, replace(left(pa.ADDRESS1, 30),',','') AS work_addr_line1
, replace(left(pa.ADDRESS2, 30),',','') AS work_addr_line2
, left(pd.MAIL_DROP, 6) AS campus_mail_code
, left(replace(replace(ltrim(rtrim(ph.PHONE + '' + ph.EXTENSION)), '-', ''), '/', ''), 10)
  AS campus_phone
, replace(left(pa.CITY, 21),',','') AS work_addr_city
, replace(left(pa.STATE, 2),',','') AS work_addr_state
, replace(left(pa.POSTAL, 9),',','') AS work_addr_zip
, case
    when
        u.EMPL_CLASS in ('3','9','10','11','14','20','21','22','23','24','24')
    and ( (  u.DESCRSHORT in ('ACT ASSOC','ACT ASST P','ACT PROF-A','ACT PROF-F','ACT PROF-F',
                             'ACT PROF-H','ACT PROF-S','ADJ PROF-A','ADJ PROF-F','ADJ PROF-H',
                             'ADJ PROF-S','ASSOC ADJ','ASSOC PROF','ASST ADJ P','ASST PROF',
                             'ASST PROF-','PROF EMERI','PROF IN RE','PROF OF CL','PROF-10 MO',
                             'PROF-AY','PROF-AY-1/','PROF-AY-B/','PROF-AY-LA','PROF-FY',
                             'PROF-FY-B/','PROF-HCOMP','PROF-SFT-V','SENATE EME','NON-SENATE',
                             'STAFF EMER','STF EMERIT','HS CLIN PR', 'HS ASST CL', 'RECALL TEA',
                             'UNIV PROF','VIS ASST P','VIS ASST P','VIS PROF','VIS PROF-H',
                             'ACT INSTR-','VISITOR-GR','VIS ASSOC')
          or u.DESCRSHORT like 'LECT%' or u.DESCRSHORT like 'SR LECT%' or u.DESCRSHORT like '%POST%'
          or u.DESCR like '%LIBRARIAN%'
          )
          or pd.EG_ACADEMIC_RANK > 0
    )
    then 4
    when u.EMPL_CLASS = '5' and u.DESCR not like '%NON-GSHIP%'
     then 3
    else 1
  end as type
, case
    when u.DEPTID in ('025000', '544500', '544600')
    then 1
    else 0
  end as law
from ucla_employees u
inner join PS_NAMES n
  on u.EMPLID = n.EMPLID
  and n.NAME_TYPE = 'PRI'
  and n.DML_IND <> 'D'
  and n.EFFDT = (
      select max(EFFDT)
      from PS_NAMES
      where EMPLID = u.EMPLID
      and NAME_TYPE = 'PRI'
      and DML_IND <> 'D'
  )
left outer join PS_EMAIL_ADDRESSES e
  on u.EMPLID = e.EMPLID
  and e.E_ADDR_TYPE = 'BUSN'
  and e.PREF_EMAIL_FLAG = 'Y'
  and e.DML_IND <> 'D'
left outer join PS_PERSONAL_PHONE ph
  on u.EMPLID = ph.EMPLID
  and ph.PHONE_TYPE = 'BUSN'
  and ph.DML_IND <> 'D'
left outer join PS_ADDRESSES pa
  on u.EMPLID = pa.EMPLID
  and pa.ADDRESS_TYPE = 'HOME'
  and pa.DML_IND <> 'D'
  and pa.EFFDT = (
      select max(EFFDT)
      from PS_ADDRESSES
      where EMPLID = pa.EMPLID
      and ADDRESS_TYPE = 'HOME'
      and DML_IND <> 'D'
  )
left outer join PS_POSITION_DATA pd
  on u.POSITION_NBR = pd.POSITION_NBR
  and pd.DML_IND <> 'D'
  and pd.EFFDT = (
      select max(EFFDT)
      from PS_POSITION_DATA
      where POSITION_NBR = pd.POSITION_NBR
      and DML_IND <> 'D'
      )
;
"""


def main():
    server = UCPATH["server"]
    database = UCPATH["database"]
    username = UCPATH["username"]
    password = UCPATH["password"]
    conn = pymssql.connect(server, username, password, database)
    cursor = conn.cursor(as_dict=True)

    ucpath_query = get_ucpath_query()
    cursor.execute(ucpath_query)
    for row in cursor:
        print(row)
    conn.close()


if __name__ == "__main__":
    main()
