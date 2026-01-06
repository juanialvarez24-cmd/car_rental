-- Query 1: Total rentals for high-rated eco-friendly vehicles
-- Business Goal: Identifies the performance of the most successful sustainable fleet.
SELECT
    SUM(rentertripstaken) AS cantidad_alquileres_ecologicos
FROM
    car_rental_db.car_rental_analytics
WHERE
    fuelType IN ('hybrid', 'electric')
    AND rating >= 4;

-- Query 2: Top 5 states with lowest rental activity
-- Business Goal: Helps identify underperforming markets that may require more marketing or inventory.
SELECT
    state_name,
    SUM(renterTripsTaken) AS total_alquileres
FROM
    car_rental_db.car_rental_analytics
GROUP BY
    state_name
ORDER BY
    total_alquileres ASC
LIMIT 5;

-- Query 3: Top 10 most rented car makes and models
-- Business Goal: Shows market demand and which specific car models are the most popular among users
SELECT
    make,
    model,
    SUM(renterTripsTaken) AS total_alquileres
FROM
    car_rental_db.car_rental_analytics
GROUP BY
    make,
    model
ORDER BY
    total_alquileres DESC
LIMIT 10;

-- Query 4: Rental volume for vehicles manufactured (2010-2015)
-- Business Goal: Evaluates the profitability and usage frequency of the older segment of the fleet.
SELECT
    year,
    SUM(renterTripsTaken) AS total_alquileres
FROM
    car_rental_db.car_rental_analytics
WHERE
    year BETWEEN 2010 AND 2015
GROUP BY
    year
ORDER BY
    year ASC;

-- Query 5: Top 5 cities for hybrid and electric rentals
-- Business Goal: Pinpoints urban areas with the highest adoption and demand for sustainable transportation.
SELECT
    city,
    SUM(renterTripsTaken) AS total_alquileres_ecologicos
FROM
    car_rental_db.car_rental_analytics
WHERE
    fuelType IN ('hybrid', 'electric')
GROUP BY
    city
ORDER BY
    total_alquileres_ecologicos DESC
LIMIT 5;

-- Query 6: Average review count segmented by fuel type
-- Business Goal: Measures customer engagement and feedback levels across different engine technologies.
SELECT
    fuelType,
    ROUND(AVG(reviewCount), 2) AS promedio_reviews    
FROM
    car_rental_db.car_rental_analytics
GROUP BY
    fuelType
ORDER BY
    promedio_reviews DESC;
