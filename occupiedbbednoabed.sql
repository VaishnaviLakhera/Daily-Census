CREATE
OR REPLACE VIEW "reporting"."occupiedbbednoabed" AS
SELECT
   v.campusabbreviation,
   v.levelofcareabbreviation,
   v.buildingname,
   v.roomname,
   bbed.admissionname
FROM
   dim.bedoccupancy bo
   JOIN dim.vwbed v ON v.bedid = bo.bedid
   JOIN (
      SELECT
         b.roomid,
         a.admissionname
      FROM
         dim.bedoccupancy bo
         JOIN dim.bed b ON b.bedid = bo.bedid
         JOIN dim.admission a ON a.admissionid = bo.admissionid
         JOIN dim.levelofcare l ON l.levelofcareid = a.levelofcareid
      WHERE
         l.levelofcareabbreviation:: text <> 'SNF':: text
         AND bo.date = date_add(
            'd':: text,
            -1:: bigint,
            'now':: text:: date:: timestamp without time zone
         )
         AND bo.occupied = 1
         AND b.issecondperson = 1
   ) bbed ON bbed.roomid = v.roomid
WHERE
   v.levelofcareabbreviation:: text <> 'SNF':: text
   AND bo.date = date_add(
      'd':: text,
      -1:: bigint,
      'now':: text:: date:: timestamp without time zone
   )
   AND bo.occupied = 0
   AND v.issecondperson = 0
   AND bo.available = 1;
