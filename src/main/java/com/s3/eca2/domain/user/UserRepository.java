package com.s3.eca2.domain.user;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Date;
import java.util.List;

public interface UserRepository extends JpaRepository<User,Long> {
    @Query("SELECT t FROM User t WHERE t.regDate >= :start AND t.regDate <= :end")
    List<User> findByRegDateBetweenOrModDateBetween(@Param("start") Date start, @Param("end") Date end);
}
