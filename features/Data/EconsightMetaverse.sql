SELECT 'EconsightMetaverse' AS vendor, * FROM [vendor].[tvfEconsightTechnologyTrend] ({}, 'EconsightMetaverse')
UNION 
SELECT  'EconsightLithiumBatteries',* FROM [vendor].[tvfEconsightTechnologyTrend] ({}, 'EconsightLithiumBatteries') 
UNION 
SELECT  'EconsightEnergyPatent',* FROM [vendor].[tvfEconsightTechnologyTrend] ({}, 'EconsightEnergyPatent')
