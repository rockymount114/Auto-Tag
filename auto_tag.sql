WITH cad_cte AS(
       SELECT inm.inci_id, inm.case_id, inm.naturecode, inm.nature, inm.closecode, inm.street, 
	          inm.citydesc, inm.zip, inm.agency, inm.calltime, offd_tbl.emdept_id, offd_tbl.timestamp AS "timeDisptached"
			  --, inm.geox, inm.geoy
       FROM cad.dbo.inmain inm
              JOIN (SELECT MIN(id.timestamp) AS timestamp, ud.emdept_id, id.inci_id
                     FROM cad.dbo.incilog id 
                           RIGHT JOIN cad.dbo.unitper ud ON id.unitperid = ud.unitperid
                           JOIN cad.dbo.emmain em ON ud.officerid = em.empl_id
                     WHERE (id.transtype = 'D') --D = Dispatched Time  E = Enroute Time   A = Arrived Time
					 --AND [agency] = ''   --Agency ID goes here
                     GROUP BY ud.emdept_id, id.inci_id
              ) AS offd_tbl ON inm.inci_id = offd_tbl.inci_id
),
offc_cte AS(
       SELECT ic.inci_id, MAX(ic.timestamp) AS "timeCleared", uc.emdept_id
       FROM cad.dbo.incilog ic 
              RIGHT JOIN cad.dbo.unitper uc ON ic.unitperid = uc.unitperid
              JOIN cad.dbo.emmain em ON uc.officerid = em.empl_id
       WHERE (ic.transtype IN ('C','X')) AND uc.emdept_id <> '-9999' 
	   --AND [agency] = ''   --Agency ID goes here
       GROUP BY ic.inci_id, uc.emdept_id
)



SELECT 

    cc.inci_id AS [Event ID], 
	CASE WHEN cc.case_id !='' THEN CONCAT('RMPD',cc.case_id) ELSE cc.case_id END AS [Report Number],
	cc.emdept_id AS [Officer Badge ID],
	convert(varchar,cc.timeDisptached, 120) AS [Officer Dispatched DateTime], 
	convert(varchar,oc.timeCleared, 120) AS [Officer Cleared DateTime],
	cc.street AS [Street], 

	CASE WHEN cc.citydesc = '' THEN 'ROCKY MOUNT' 
		 WHEN cc.citydesc = 'NV' THEN 'NASHVILLE'
		 ELSE cc.citydesc END AS [City], 


	'NC' AS [State], 
     
	 '' AS [Zip Code], --Add Zip Code here
	 --cc.geox,
	 --cc.geoy,

	(SELECT inmain.nature FROM inmain WHERE inmain.inci_id = cc.inci_id) AS [Call Type],
	cc.closecode AS [Clearance Code],
       (SELECT inmain.nature FROM inmain WHERE inmain.inci_id = cc.inci_id) AS [Category]
       --, cc.closecode AS [Category 2]
	  , CONCAT(cc.street,', ', cc.citydesc) AS 'Custom Address'
FROM cad_cte cc

JOIN offc_cte oc 
ON cc.inci_id = oc.inci_id AND cc.emdept_id = oc.emdept_id


WHERE cc.calltime >= DATEADD(dd, -2, GETDATE())

ORDER BY cc.inci_id