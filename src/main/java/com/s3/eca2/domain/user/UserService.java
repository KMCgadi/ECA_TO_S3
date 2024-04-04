package com.s3.eca2.domain.user;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository){
        this.userRepository = userRepository;
    }

    public User find(long userEid) {
        return userRepository.findById(userEid)
                .orElseThrow(() -> new EntityNotFoundException("User not found for id:" + userEid));
    }

    public Page<User> findUserByDate(Date start, Date end, Pageable pageable) {
        return userRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
