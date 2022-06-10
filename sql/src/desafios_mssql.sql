SELECT SP.CountryRegionCode, AVG(TR.TaxRate) AS average_TaxRate
FROM Sales.SalesTaxRate AS TR
INNER JOIN Person.StateProvince AS SP ON SP.StateProvinceID = TR.StateProvinceID
GROUP BY SP.CountryRegionCode;

select  coun_reg.Name as country_name, c.Name as currency_name, 
        cast(max(cr.AverageRate) as numeric(10, 2)) as AverageRate, 
        cast(avg(st_rate.TaxRate) as numeric(10, 2)) as TaxRate
from Person.StateProvince as sp
inner join Person.CountryRegion as coun_reg on coun_reg.CountryRegionCode = sp.CountryRegionCode
inner join Sales.CountryRegionCurrency as crc on crc.CountryRegionCode = sp.CountryRegionCode
inner join Sales.Currency as c on c.CurrencyCode = crc.CurrencyCode
inner join Sales.CurrencyRate as cr on cr.ToCurrencyCode = crc.CurrencyCode
inner join Sales.SalesTaxRate as st_rate on st_rate.StateProvinceID = sp.StateProvinceID
group by coun_reg.Name, c.Name;