package com.weatheretl.repository;

import com.weatheretl.model.output.WeatherRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Repository
public interface WeatherRepository extends JpaRepository<WeatherRecord, Long> {

    Optional<WeatherRecord> findByDateAndLatitudeAndLongitude(
            LocalDate date, Double latitude, Double longitude);

    List<WeatherRecord> findByDateBetween(LocalDate startDate, LocalDate endDate);

    List<WeatherRecord> findByDateBetweenAndLatitudeAndLongitude(
            LocalDate startDate, LocalDate endDate, Double latitude, Double longitude);

    boolean existsByDateAndLatitudeAndLongitude(
            LocalDate date, Double latitude, Double longitude);

    long countByDateBetween(LocalDate startDate, LocalDate endDate);

    @Query("SELECT DISTINCT w.latitude, w.longitude FROM WeatherRecord w")
    List<Object[]> findDistinctLocations();

    @Modifying
    @Transactional
    @Query("DELETE FROM WeatherRecord w WHERE w.date BETWEEN :startDate AND :endDate")
    int deleteByDateBetween(@Param("startDate") LocalDate startDate,
                            @Param("endDate") LocalDate endDate);
}