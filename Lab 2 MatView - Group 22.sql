 --MatView_MultipleParcelOwners table for the 2025 tax assessment year.

--Clear existing records from MatView_MultipleParcelOwners
DELETE FROM MatView_MultipleParcelOwners 
WHERE AssessmentYear = 2025;

-- Temporary Table
DROP TABLE IF EXISTS TempMatView_MultipleParcelOwners;

CREATE TEMPORARY TABLE TempMatView_MultipleParcelOwners AS
SELECT
    AssessmentYear,                          
    COUNT(ParcelID) AS NumberOfParcels,      
    SUM(NumberOfBuildings) AS NumberOfBuildings, 
    OwnerName AS Owner,                   
    SUM(GrossSqFeet) AS TotalGrossSqFeet,   
    SUM(LandAreaSqFeet) AS TotalLandSqFeet,  
    SUM(TotalTaxableAssessedValue) AS TotalTaxableValue, 
    SUM(TotalUnits) AS TotalUnits           
FROM TaxAssessment
WHERE 
    OwnerName NOT LIKE '%UNKNOWN%'
    AND OwnerName NOT LIKE '%UNNAMED%'
    AND OwnerName NOT LIKE '%OWNER%'
    AND OwnerName NOT LIKE '%NAME%'
    AND NOT REGEXP('^[`0-9~!@#$%^\&*()\-_=+{}\[\]\|\:;"<>,.//\\?]', OwnerName)
    AND OwnerName IS NOT NULL
    AND AssessmentYear = 2025 
GROUP BY OwnerName
HAVING COUNT(ParcelID) > 1
ORDER BY NumberOfParcels;

-- Load data 
INSERT INTO MatView_MultipleParcelOwners (
    AssessmentYear,
    NumberOfParcels,
    NumberOfBuildings,
    Owner,
    TotalGrossSqFeet,
    TotalLandSqFeet,
    TotalTaxableValue,
    TotalUnits
)
SELECT 
    AssessmentYear,
    NumberOfParcels,
    NumberOfBuildings,
    Owner,
    TotalGrossSqFeet,
    TotalLandSqFeet,
    TotalTaxableValue,
    TotalUnits
FROM TempMatView_MultipleParcelOwners;

--Clean up temporary storage
DROP TABLE IF EXISTS TempMatView_MultipleParcelOwners;



-- MatView_BlockSummaries table

-- Clear records
DELETE FROM MatView_BlockSummaries 
WHERE AssessmentYear = 2025;

-- Temporary Table
DROP TABLE IF EXISTS TempMatView_BlockSummaries;

CREATE TEMPORARY TABLE TempMatView_BlockSummaries AS
SELECT 
    ta.Block AS Block,                       
    CASE 
        WHEN ta.BoroughID = 1 THEN 'Manhattan'
        WHEN ta.BoroughID = 2 THEN 'Bronx'
        WHEN ta.BoroughID = 3 THEN 'Brooklyn'
        WHEN ta.BoroughID = 4 THEN 'Queens'
        WHEN ta.BoroughID = 5 THEN 'Staten Island'
        ELSE 'Unknown'
    END AS BoroughName,                     
    SUM(ta.GrossSqFeet) AS TotalGrossSqFeet,  
    SUM(ta.LandAreaSqFeet) AS TotalLandSqFeet, 
    COUNT(DISTINCT ta.NumberOfBuildings) AS NumberOfBuildings, 
    COUNT(DISTINCT ta.Lot) AS NumberOfLots,  
    SUM(ta.TotalUnits) AS NumberOfUnits,      
    SUM(ta.TotalTaxableAssessedValue) AS TotalTaxableValue, 
    ta.AssessmentYear AS AssessmentYear      
FROM 
    TaxAssessment AS ta
WHERE 
    ta.AssessmentYear = 2025 
GROUP BY 
    ta.Block, 
    ta.BoroughID;           

-- Load data into MatView_BlockSummaries
INSERT INTO MatView_BlockSummaries (
    Block,
    BoroughName,
    TotalGrossSqFeet,
    TotalLandSqFeet,
    NumberOfBuildings,
    NumberOfLots,
    NumberOfUnits,
    TotalTaxableValue,
    AssessmentYear
)
SELECT 
    Block,
    BoroughName,
    TotalGrossSqFeet,
    TotalLandSqFeet,
    NumberOfBuildings,
    NumberOfLots,
    NumberOfUnits,
    TotalTaxableValue,
    AssessmentYear
FROM TempMatView_BlockSummaries;

-- Clean up temporary storage
DROP TABLE IF EXISTS TempMatView_BlockSummaries;
