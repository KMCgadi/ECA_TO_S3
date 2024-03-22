package com.s3.eca2.domain.user;

import com.s3.eca2.domain.toastHistory.ToastHistory;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

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

    public List<User> findUserByDate(Date start, Date end) {
        return userRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
