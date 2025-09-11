-- =====================================================
-- Script: Reset Medallion Architecture Databases in MySQL
-- Purpose: This script drops and recreates the Bronze, Silver, 
--          and Gold databases used to model the data warehouse layers.
--
-- ⚠️ WARNING: Running this script will permanently delete all 
-- existing data inside these databases. Use with caution!
-- =====================================================

-- Drop databases if they already exist
DROP DATABASE IF EXISTS Bronze_Datawarehouse;
DROP DATABASE IF EXISTS Silver_Datawarehouse;
DROP DATABASE IF EXISTS Gold_Datawarehouse;

-- Recreate fresh ones
CREATE DATABASE Bronze_Datawarehouse;
CREATE DATABASE Silver_Datawarehouse;
CREATE DATABASE Gold_Datawarehouse;




