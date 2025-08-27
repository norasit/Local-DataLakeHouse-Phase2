-- เลือกคาตาล็อกและเนมสเปซให้ชัดก่อน
USE CATALOG iceberg;   -- (หรือ lh ก็ได้ เพราะชี้ไป HMS เดียวกัน)
USE lh;                -- นี่คือ schema ใน HMS

-- === Iceberg maintenance ===
-- รวมไฟล์เล็ก (bin-pack compaction)
CALL system.rewrite_data_files('nyc_yellow');

-- จัดระเบียบ manifest
CALL system.rewrite_manifests('nyc_yellow');

-- ลบสแนปช็อตเก่า (ปรับเวลาเป็นของคุณ)
CALL system.expire_snapshots('nyc_yellow', TIMESTAMP '2025-08-01 00:00:00');

-- ทางเลือก: Optimize ระดับไฟล์ (ถ้าอยากเรียกผ่าน SQL)
ALTER TABLE nyc_yellow EXECUTE optimize;

-- ทางเลือก: ลบ orphan files (ใช้ด้วยความระวัง)
-- CALL system.remove_orphan_files('nyc_yellow', TIMESTAMP '2025-08-01 00:00:00');
