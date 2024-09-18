select
  container_number,
  count(*)
from export_nw
group by container_number