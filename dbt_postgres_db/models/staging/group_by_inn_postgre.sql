select
  company_inn,
  company_name_unified
from marketing.reference_inn
group by company_inn, company_name_unified