select
  container_number,
  count(*)
from marketing.export_nw
group by container_number