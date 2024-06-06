with total_inv as (select
                  p21_view_inv_mast.inv_mast_uid,
                  SUM(COALESCE(p21_view_inv_loc.qty_on_hand, 0)) AS total_inv_QOH,
                  MAX(p21_view_inv_loc.last_purchase_date) AS LPD
                  FROM p21_view_inv_mast
                  Left JOIN p21_view_inv_loc ON p21_view_inv_mast.inv_mast_uid = p21_view_inv_loc.inv_mast_uid
                  GROUP BY
                  p21_view_inv_mast.inv_mast_uid
                  )
select
p21_item_view.item_id,
p21_item_view.item_desc,
p21_item_view.inv_mast_uid,
p21_item_view.supplier_name,
p21_item_view.supplier_part_no as Supplier_part_number,
p21_item_view.short_code,
p21_item_view.delete_flag,
MIN(p21_item_view.cost) AS Cost,
MAX(p21_item_view.price1) AS P1,
MAX(P21_item_view.list_price) AS LIST_PRICE,
inv_mast_ud.on_vendor_price_book,
total_inv.total_inv_QOH,
total_inv.LPD,
COUNT(l.location_id) AS stockable_locations_count
--, stuff ((
--                                        select ', ' + la.
--                                        from p21_view_inv_loc ill
--                                        inner join p21_view_address la on ill.location_id = la.id
--                                        where ill.inv_mast_uid = il.inv_mast_uid
--                                        and ill.delete_flag = 'N'
--                                        and ill.qty_on_hand-ill.qty_allocated-qty_backordered > 0
--                                        for xml path('')
--                                        ), 1, 1, '') as avail_location_list


FROM
p21_item_view

LEFT JOIN
    p21_view_inv_loc l ON p21_item_view.inv_mast_uid = l.inv_mast_uid AND l.stockable = 'Y'
LEFT JOIN
      inv_mast_ud ON p21_item_view.inv_mast_uid = inv_mast_ud.inv_mast_uid
LEFT JOIN
      total_inv ON p21_item_view.inv_mast_uid = total_inv.inv_mast_uid
WHERE
p21_item_view.supplier_id IN ('183938')
AND p21_item_view.delete_flag = 'N'

GROUP BY
p21_item_view.item_id,
p21_item_view.item_desc,
p21_item_view.inv_mast_uid,
p21_item_view.supplier_name,
p21_item_view.supplier_part_no,
p21_item_view.short_code,
p21_item_view.delete_flag,
inv_mast_ud.on_vendor_price_book,
total_inv.total_inv_QOH,
total_inv.LPD

ORDER BY
inv_mast_ud.on_vendor_price_book, p21_item_view.item_id